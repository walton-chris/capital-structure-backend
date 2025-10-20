import os
import base64
import uuid
import json
import io
import re
import tempfile
import logging
import binascii
from typing import List, Optional, Dict, Any

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
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", "25000000"))
ALLOWED_EXTS = {"xlsx"}

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
    version="6.3.0-robust-parser",
    description="API with improved parsing, prompting, and error handling."
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
        if not v or not v.strip(): raise ValueError("The 'name' field for a security cannot be empty.")
        return v.strip()

class CapitalStructureInput(BaseModel):
    securities: List[Security]
    total_option_pool_shares: NonNegativeFloat = Field(ge=0)

# ... other request/response models ...
class FileUploadRequest(BaseModel): file_content: str; file_name: str
class DocumentExtractRequest(BaseModel): file_id: str
class DocumentUploadResponse(BaseModel): file_id: str; file_name: str; message: str; file_size_bytes: int


# =============================================================================
# 5. "LLM-First" Anonymizer & Parser
# =============================================================================

class Anonymizer:
    """Manages the state of anonymization to ensure consistent replacements."""
    def __init__(self):
        self.name_map: Dict[str, str] = {}
        self.person_counter = 1
        self.entity_counter = 1
        self.person_pattern = re.compile(r'\b[A-Z][a-z]+,?\s[A-Z][a-zA-Z\.\-]+\b')
        self.entity_pattern = re.compile(r'\b[A-Z][A-Za-z\s,&]+\s(?:LLC|Inc|LP|FBO|Capital|Partners|Fund|Trust|Ventures|Co\.)\b')

    def _get_placeholder(self, name: str, is_entity: bool) -> str:
        if name in self.name_map: return self.name_map[name]
        placeholder = f"Entity-{self.entity_counter}" if is_entity else f"Person-{self.person_counter}"
        if is_entity: self.entity_counter += 1
        else: self.person_counter += 1
        self.name_map[name] = placeholder
        return placeholder

    def anonymize_cell(self, cell_content: str) -> str:
        if not isinstance(cell_content, str) or not cell_content.strip(): return cell_content
        anonymized = self.entity_pattern.sub(lambda m: self._get_placeholder(m.group(0), True), cell_content)
        anonymized = self.person_pattern.sub(lambda m: self._get_placeholder(m.group(0), False), anonymized)
        return anonymized.replace("Proof Holdings Inc.", "[The Company]")

def process_and_anonymize_excel(file_bytes: bytes) -> str:
    """Safely converts an Excel file to anonymized Markdown, trimming empty rows."""
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
        anonymizer = Anonymizer()
        markdown_parts = []
        for sheet in workbook.worksheets:
            if sheet.max_row == 0: continue
            markdown_parts.append(f"## Sheet: {sheet.title}\n")
            
            # IMPROVEMENT: Filter out empty rows before processing
            raw_rows = [list(row) for row in sheet.iter_rows(values_only=True) if any(cell is not None for cell in row)]
            if not raw_rows: continue

            anonymized_rows = [[anonymizer.anonymize_cell(str(cell) if cell is not None else "") for cell in row] for row in raw_rows]
            
            header = anonymized_rows[0]
            separator = ["---" for _ in header]
            markdown_parts.append(f"| {' | '.join(header)} |")
            markdown_parts.append(f"| {' | '.join(separator)} |")
            for row in anonymized_rows[1:]:
                if len(row) < len(header): row.extend([""] * (len(header) - len(row)))
                markdown_parts.append(f"| {' | '.join(row[:len(header)])} |")
            markdown_parts.append("\n")
        return "\n".join(markdown_parts)
    except Exception as e:
        logger.exception("Failed during Excel to Markdown conversion/anonymization.")
        raise ValueError(f"Could not parse and anonymize the Excel file. Error: {e}")


# =============================================================================
# 6. LLM Integration
# =============================================================================

# IMPROVEMENT: Added negative constraints to make the prompt more robust.
EXTRACTION_SYSTEM_PROMPT = """You are an expert financial analyst... (Full prompt from previous version) ...

**CRITICAL INSTRUCTIONS**:
- ... (previous instructions) ...
- **IMPORTANT**: Ignore any rows in the tables that are clearly totals, sub-totals, or summary rows. A valid security entry must have a specific name and a corresponding number of shares. Do not create entries from rows that lack this information.
- Your final output must be a single, valid JSON object and nothing else.
"""

async def call_llm(document_text: str) -> Dict[str, Any]:
    # This function's logic is sound and remains the same.
    if client is None: raise HTTPException(status_code=503, detail="AI service is not configured.")
    try:
        # ... (API call logic remains the same) ...
        def _do_call():
            return client.chat.completions.create(
                model="gpt-4o", temperature=0.0,
                response_format={"type": "json_object"},
                max_tokens=4096,
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Here is the anonymized cap table data... \n\n{document_text}"},
                ],
                timeout=120.0,
            )
        resp = await run_in_threadpool(_do_call)
        content = resp.choices[0].message.content
        if not content: raise HTTPException(status_code=502, detail="AI returned empty response.")
        return json.loads(content)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse LLM JSON. Content: {content[:500]}")
        raise HTTPException(status_code=502, detail="AI returned malformed JSON.")
    except Exception as e:
        logger.error(f"OpenAI API call failed: {e}")
        raise HTTPException(status_code=503, detail="AI service is unavailable or timed out.")


# =============================================================================
# 7. API Routes
# =============================================================================

@app.get("/", summary="API Root", tags=["Health"])
async def root(): return {"message": "Capital Structure API", "version": app.version}

@app.get("/health", summary="Health Check", tags=["Health"])
async def health(): return {"status": "healthy"}

@app.post("/api/documents/upload", response_model=DocumentUploadResponse, status_code=201, tags=["Document Processing"])
async def upload_document(request: FileUploadRequest):
    # This endpoint is stable and remains the same.
    encoded_len = len(request.file_content)
    if (encoded_len * 3 / 4) > MAX_UPLOAD_BYTES: raise HTTPException(status_code=413, detail="File is too large.")
    try:
        file_bytes = base64.b64decode(request.file_content, validate=True)
    except (ValueError, binascii.Error):
        raise HTTPException(status_code=400, detail="Invalid base64 content.")
    if len(file_bytes) > MAX_UPLOAD_BYTES: raise HTTPException(status_code=413, detail="File size exceeds limit.")
    ext = request.file_name.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_EXTS: raise HTTPException(status_code=415, detail="Please upload an .xlsx file.")
    file_id = f"upload_{uuid.uuid4()}.{ext}"
    try:
        with tempfile.NamedTemporaryFile(delete=False, prefix="cap_struct_", suffix=f"_{file_id}", dir="/tmp") as tmp:
            tmp.write(file_bytes)
            path = tmp.name
        file_storage[file_id] = {"path": path, "original_name": request.file_name}
        return DocumentUploadResponse(file_id=file_id, file_name=request.file_name, message="File uploaded", file_size_bytes=len(file_bytes))
    except Exception as e:
        logger.exception(f"Failed to write temp file: {e}")
        raise HTTPException(status_code=500, detail="Failed to save file on server.")

@app.post("/api/documents/extract", response_model=CapitalStructureInput, tags=["Document Processing"])
async def extract_data(payload: DocumentExtractRequest):
    rid = str(uuid.uuid4())[:8]
    logger.info(f"[rid={rid}] Extraction requested for file_id={payload.file_id}")
    if payload.file_id not in file_storage: raise HTTPException(status_code=404, detail="File not found.")
    path = file_storage[payload.file_id].get("path")
    if not path or not os.path.exists(path):
        logger.error(f"[rid={rid}] File missing at path: {path}")
        raise HTTPException(status_code=410, detail="File has expired.")
    
    llm_obj = None # Define here for access in exception blocks
    try:
        def _read_and_process():
            with open(path, "rb") as f:
                return process_and_anonymize_excel(f.read())
        
        document_text = await run_in_threadpool(_read_and_process)
        llm_obj = await call_llm(document_text)
        result = CapitalStructureInput.model_validate(llm_obj)
        
        if not result.securities:
            raise HTTPException(status_code=502, detail="AI returned a valid but empty list of securities.")
        return result

    # REFACTORED: New, specific error handling for Pydantic validation failures.
    except ValidationError as e:
        error_details = e.errors()
        logger.error(f"[rid={rid}] AI response failed Pydantic validation. Details: {error_details}. AI Response: {llm_obj}")
        # Provide a clean, specific error message to the frontend.
        first_error = error_details[0]
        field = " -> ".join(map(str, first_error['loc']))
        msg = first_error['msg']
        raise HTTPException(status_code=400, detail=f"AI returned invalid data. Field: '{field}', Error: {msg}")
    
    except (ValueError, HTTPException) as e:
        # Catch our own explicit errors (like from parsing) or re-raise HTTPExceptions.
        status_code = e.status_code if isinstance(e, HTTPException) else 400
        detail = e.detail if isinstance(e, HTTPException) else str(e)
        logger.warning(f"[rid={rid}] Handled error during extraction: {detail}")
        raise HTTPException(status_code=status_code, detail=detail)

    except Exception:
        logger.exception(f"[rid={rid}] An unexpected, unhandled error occurred during extraction.")
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
