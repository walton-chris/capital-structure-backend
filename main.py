import os
import base64
import uuid
import json
import io
import re
import tempfile
import logging
import binascii
from typing import List, Optional, Dict, Any, Tuple

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field, NonNegativeFloat, field_validator, ValidationError
from openai import OpenAI
import openpyxl

# =============================================================================
# 1. Logging & Configuration
# =============================================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d] - %(message)s",
)
logger = logging.getLogger("capital-structure")

PORT = int(os.getenv("PORT", "8080"))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", "25000000"))  # 25 MB
ALLOWED_EXTS = {"xlsx"} # NOTE: This approach is tailored for Excel files now.

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client: Optional[OpenAI] = None
if OPENAI_API_KEY:
    client = OpenAI(api_key=OPENAI_API_KEY)
else:
    logger.critical("FATAL: OPENAI_API_KEY environment variable is not set.")


# =============================================================================
# 2. FastAPI App & Middleware
# =============================================================================

app = FastAPI(
    title="Capital Structure API",
    version="6.0.0-llm-first",
    description="API for extracting structured data from financial documents using an LLM-first approach."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173", "http://localhost:3000",
        "https://capital-structure-frontend.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)


# =============================================================================
# 3. In-Memory Storage
# =============================================================================

file_storage: Dict[str, Dict[str, Any]] = {}


# =============================================================================
# 4. Pydantic Models
# =============================================================================
# NOTE: The output models remain the same, as our API's contract with the frontend hasn't changed.
class Security(BaseModel):
    name: str
    shares_outstanding: NonNegativeFloat = Field(ge=0)
    original_investment_per_share: NonNegativeFloat = Field(ge=0)
    liquidation_preference_multiple: NonNegativeFloat = Field(ge=0)
    seniority: Optional[int] = Field(default=None, ge=0, le=10)
    is_participating: bool
    participation_cap_multiple: NonNegativeFloat = Field(ge=0)
    cumulative_dividend_rate: NonNegativeFloat = Field(ge=0)
    years_since_issuance: NonNegativeFloat = Field(ge=0)

    @field_validator("name")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v or not v.strip(): raise ValueError("name field cannot be empty")
        return v.strip()

class CapitalStructureInput(BaseModel):
    securities: List[Security]
    total_option_pool_shares: NonNegativeFloat = Field(ge=0)

class FileUploadRequest(BaseModel):
    file_content: str
    file_name: str

class DocumentExtractRequest(BaseModel):
    file_id: str

class DocumentUploadResponse(BaseModel):
    file_id: str
    file_name: str
    message: str
    file_size_bytes: int


# =============================================================================
# 5. NEW "LLM-First" Helper Functions
# =============================================================================

def excel_to_markdown(file_bytes: bytes) -> str:
    """Converts all sheets in an Excel workbook to a single Markdown string."""
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
        markdown_parts = []
        for sheet in workbook.worksheets:
            # Skip empty sheets
            if sheet.max_row == 0 or sheet.max_column == 0:
                continue
            
            markdown_parts.append(f"## Sheet: {sheet.title}\n")
            
            rows = list(sheet.iter_rows(values_only=True))
            if not rows: continue

            # Create header and separator
            header = [str(cell) if cell is not None else "" for cell in rows[0]]
            separator = ["---" for _ in header]
            
            markdown_parts.append(f"| {' | '.join(header)} |")
            markdown_parts.append(f"| {' | '.join(separator)} |")

            # Add content rows
            for row in rows[1:]:
                content = [str(cell) if cell is not None else "" for cell in row]
                # Ensure row has same number of columns as header
                if len(content) < len(header):
                    content.extend([""] * (len(header) - len(content)))
                elif len(content) > len(header):
                    content = content[:len(header)]
                markdown_parts.append(f"| {' | '.join(content)} |")
            
            markdown_parts.append("\n") # Add space between sheets
            
        return "\n".join(markdown_parts)
    except Exception as e:
        logger.error(f"Failed to convert Excel to Markdown: {e}")
        raise ValueError(f"Could not parse the provided Excel file. It may be corrupted or in an unsupported format. Error: {e}")

def anonymize_text(text_content: str) -> str:
    """Finds and replaces names of people and entities with generic placeholders."""
    # This is a heuristic-based approach. It can be improved with more sophisticated NLP techniques.
    name_map = {}
    person_counter = 1
    entity_counter = 1

    # Pattern for individual names (e.g., "John Doe", "J. Doe", "DOE, JOHN")
    # This is a simplified pattern and may not catch all cases.
    person_pattern = re.compile(r'\b[A-Z][a-z]+ [A-Z][a-zA-Z\.]+\b|\b[A-Z]{2,}, [A-Z][a-z]+\b')
    
    # Pattern for entities (e.g., "VC Fund LP", "Abraham & Co. Inc.")
    entity_pattern = re.compile(r'\b[A-Z][A-Za-z\s,&]+\s(?:LLC|Inc|LP|FBO|Capital|Partners|Fund|Trust|Ventures)\b')

    def get_placeholder(match):
        nonlocal person_counter, entity_counter
        name = match.group(0).strip()
        if name in name_map:
            return name_map[name]
        
        # Simple heuristic: if it contains a common entity suffix, treat as entity
        if any(suffix in name for suffix in ["LLC", "Inc", "LP", "FBO", "Capital", "Partners", "Fund", "Trust"]):
            placeholder = f"Entity-{entity_counter}"
            entity_counter += 1
        else:
            placeholder = f"Person-{person_counter}"
            person_counter += 1
        
        name_map[name] = placeholder
        return placeholder

    anonymized_content = entity_pattern.sub(get_placeholder, text_content)
    anonymized_content = person_pattern.sub(get_placeholder, anonymized_content)
    
    # Also replace the specific company name from the provided files
    anonymized_content = anonymized_content.replace("Proof Holdings Inc.", "[The Company]")

    return anonymized_content

# =============================================================================
# 6. LLM Integration (with new Prompt)
# =============================================================================

EXTRACTION_SYSTEM_PROMPT = """You are an expert financial analyst specializing in venture capital cap tables. You will be given the full contents of a company's capitalization table from an Excel workbook, converted to Markdown format. Each sheet is separated by a `## Sheet: ...` header.

Your task is to meticulously analyze all the provided sheets and perform the following actions:

1.  **Identify Key Sheets**: Locate the primary "Detailed Cap Table" and the "Option Plan" or equivalent ledger. The data may be spread across multiple sheets.

2.  **Extract All Securities**: From the main capitalization table, identify every class of security (e.g., "Common Stock", "Series Seed Preferred", "Series A Preferred Stock", etc.). For each, extract the total number of outstanding shares.

3.  **Extract Option Details**: From the option ledger sheet, find all outstanding options. You **MUST** group these options by their unique **Exercise Price**.

4.  **Consolidate and Structure**: Compile all the extracted information into a single, structured JSON object that strictly adheres to the provided schema.

**CRITICAL INSTRUCTIONS**:
- Create a **separate** security entry in the final JSON for each distinct option group based on its exercise price. The name must be "Options at $X.XX Exercise Price".
- **DO NOT** create a single, aggregated security for all options like "Options Outstanding".
- The stakeholder names in the input data have been anonymized for privacy. Do not mention "Person-1", "Entity-A", etc., in your output.
- All numerical fields in the output JSON must be numbers, not strings.

Return ONLY the final JSON object. Do not include any explanations, apologies, or markdown formatting.
"""

async def call_llm(document_text: str) -> Dict[str, Any]:
    # This function remains largely the same, but it's now sending much more text.
    if client is None:
        raise HTTPException(status_code=503, detail="AI service is not configured on the server.")
    try:
        def _do_call():
            return client.chat.completions.create(
                model="gpt-4o", # Using a more powerful model for this complex task
                response_format={"type": "json_object"},
                temperature=0.0, # Set to 0 for maximum predictability
                max_tokens=4096,
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Here is the anonymized cap table data in Markdown format. Please extract the capital structure.\n\n{document_text}"},
                ],
                timeout=120.0, # Increased timeout for the larger payload
            )
        resp = await run_in_threadpool(_do_call)
        content = resp.choices[0].message.content
    except Exception as e:
        logger.error(f"OpenAI API call failed: {e}")
        raise HTTPException(status_code=503, detail="AI service is unavailable or timed out.")
    if not content:
        raise HTTPException(status_code=502, detail="AI service returned an empty response.")
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse LLM JSON. Content: {content[:500]}")
        raise HTTPException(status_code=502, detail="AI service returned a malformed response.")


# =============================================================================
# 7. API Routes (Rewired for new workflow)
# =============================================================================

@app.get("/", summary="API Root", tags=["Health"])
async def root(): return {"message": "Capital Structure API", "version": app.version}

@app.get("/health", summary="Health Check", tags=["Health"])
async def health(): return {"status": "healthy"}

@app.post("/api/documents/upload", response_model=DocumentUploadResponse, status_code=201, tags=["Document Processing"])
async def upload_document(request: FileUploadRequest):
    # This endpoint remains the same, as it's just for receiving and storing the file.
    encoded_len = len(request.file_content)
    if (encoded_len * 3 / 4) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large.")
    try:
        file_bytes = base64.b64decode(request.file_content, validate=True)
    except (ValueError, binascii.Error):
        raise HTTPException(status_code=400, detail="Invalid base64 content in payload.")
    if len(file_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File size exceeds limit of {MAX_UPLOAD_BYTES / 1_000_000} MB.")
    ext = request.file_name.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_EXTS:
        raise HTTPException(status_code=415, detail=f"Unsupported file type. Please upload an .xlsx file.")
    file_id = f"upload_{uuid.uuid4()}.{ext}"
    try:
        with tempfile.NamedTemporaryFile(delete=False, prefix="cap_struct_", suffix=f"_{file_id}", dir="/tmp") as tmp:
            tmp.write(file_bytes)
            path = tmp.name
        file_storage[file_id] = {"path": path, "original_name": request.file_name, "size": len(file_bytes)}
        return DocumentUploadResponse(file_id=file_id, file_name=request.file_name, message="File uploaded successfully", file_size_bytes=len(file_bytes))
    except Exception as e:
        logger.exception(f"Failed to write temp file: {e}")
        raise HTTPException(status_code=500, detail="Failed to save file on server.")

@app.post("/api/documents/extract", response_model=CapitalStructureInput, tags=["Document Processing"])
async def extract_data(payload: DocumentExtractRequest):
    rid = str(uuid.uuid4())[:8]
    logger.info(f"[rid={rid}] Extraction requested for file_id={payload.file_id}")
    if payload.file_id not in file_storage:
        raise HTTPException(status_code=404, detail="File not found.")
    meta = file_storage[payload.file_id]
    path = meta.get("path")
    if not path or not os.path.exists(path):
        logger.error(f"[rid={rid}] File missing at path: {path}")
        raise HTTPException(status_code=410, detail="File has expired.")
    try:
        def _process_and_anonymize():
            with open(path, "rb") as f:
                file_bytes = f.read()
            markdown_content = excel_to_markdown(file_bytes)
            anonymized_content = anonymize_text(markdown_content)
            return anonymized_content
        
        document_text = await run_in_threadpool(_process_and_anonymize)
        
        llm_obj = await call_llm(document_text)
        
        result = CapitalStructureInput.model_validate(llm_obj)
        if not result.securities:
            raise HTTPException(status_code=502, detail="AI service returned a valid but empty list of securities.")
        return result
    except ValueError as e: # Catches errors from our new excel_to_markdown helper
        raise HTTPException(status_code=400, detail=str(e))
    except ValidationError as e:
        shapes = [sorted(s.keys()) for s in (llm_obj.get("securities") or [])[:3] if isinstance(s, dict)]
        logger.error(f"[rid={rid}] AI response failed validation. Shapes: {shapes}, Errors: {e.errors()}")
        raise HTTPException(status_code=502, detail="AI response failed validation.")
    except HTTPException:
        raise
    except Exception:
        logger.exception(f"[rid={rid}] Unexpected error during extraction.")
        raise HTTPException(status_code=500, detail="An unexpected server error occurred.")
    finally:
        try:
            if path and os.path.exists(path):
                os.remove(path)
                logger.info(f"[rid={rid}] Cleaned up temp file: {path}")
            if payload.file_id in file_storage:
                del file_storage[payload.file_id]
        except Exception as e:
            logger.error(f"[rid={rid}] CRITICAL: Failed to clean up temp file {path}: {e}")

# =============================================================================
# 8. Server Entrypoint
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting server on port {PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
