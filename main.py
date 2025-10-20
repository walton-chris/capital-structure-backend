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
    version="6.1.0-safe-anonymizer",
    description="API using a robust, cell-by-cell anonymizer for the LLM-first approach."
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
# 5. NEW "LLM-First" Anonymizer & Parser
# =============================================================================

class Anonymizer:
    """Manages the state of anonymization to ensure consistent replacements."""
    def __init__(self):
        self.name_map: Dict[str, str] = {}
        self.person_counter = 1
        self.entity_counter = 1
        # More robust patterns to catch various name formats
        self.person_pattern = re.compile(r'\b[A-Z][a-z]+,?\s[A-Z][a-zA-Z\.\-]+\b')
        self.entity_pattern = re.compile(r'\b[A-Z][A-Za-z\s,&]+\s(?:LLC|Inc|LP|FBO|Capital|Partners|Fund|Trust|Ventures|Co\.)\b')

    def _get_placeholder(self, name: str, is_entity: bool) -> str:
        if name in self.name_map:
            return self.name_map[name]
        
        if is_entity:
            placeholder = f"Entity-{self.entity_counter}"
            self.entity_counter += 1
        else:
            placeholder = f"Person-{self.person_counter}"
            self.person_counter += 1
        
        self.name_map[name] = placeholder
        return placeholder

    def anonymize_cell(self, cell_content: str) -> str:
        """Anonymizes names within a single cell's string content."""
        if not isinstance(cell_content, str) or not cell_content.strip():
            return cell_content
        
        # Replace entities first as they can be more specific
        anonymized_content = self.entity_pattern.sub(lambda m: self._get_placeholder(m.group(0), True), cell_content)
        # Then replace person names
        anonymized_content = self.person_pattern.sub(lambda m: self._get_placeholder(m.group(0), False), anonymized_content)
        
        return anonymized_content.replace("Proof Holdings Inc.", "[The Company]")

def process_and_anonymize_excel(file_bytes: bytes) -> str:
    """
    Safely converts an Excel file to anonymized Markdown by processing cell by cell.
    """
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
        anonymizer = Anonymizer()
        markdown_parts = []

        for sheet in workbook.worksheets:
            if sheet.max_row == 0: continue
            
            markdown_parts.append(f"## Sheet: {sheet.title}\n")
            
            anonymized_rows = []
            for row in sheet.iter_rows(values_only=True):
                anonymized_row = [anonymizer.anonymize_cell(str(cell) if cell is not None else "") for cell in row]
                anonymized_rows.append(anonymized_row)
            
            if not anonymized_rows: continue
            
            header = anonymized_rows[0]
            separator = ["---" for _ in header]
            
            markdown_parts.append(f"| {' | '.join(header)} |")
            markdown_parts.append(f"| {' | '.join(separator)} |")

            for row in anonymized_rows[1:]:
                if len(row) < len(header):
                    row.extend([""] * (len(header) - len(row)))
                markdown_parts.append(f"| {' | '.join(row[:len(header)])} |")
            
            markdown_parts.append("\n")
            
        return "\n".join(markdown_parts)
    except Exception as e:
        logger.exception("Failed during Excel to Markdown conversion/anonymization.")
        raise ValueError(f"Could not parse and anonymize the Excel file. Error: {e}")


# =============================================================================
# 6. LLM Integration
# =============================================================================

EXTRACTION_SYSTEM_PROMPT = """You are an expert financial analyst...""" # (Same LLM-First prompt as before)

async def call_llm(document_text: str) -> Dict[str, Any]:
    if client is None:
        raise HTTPException(status_code=503, detail="AI service is not configured.")
    try:
        def _do_call():
            return client.chat.completions.create(
                model="gpt-4o", temperature=0.0,
                response_format={"type": "json_object"},
                max_tokens=4096,
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Here is the anonymized cap table data in Markdown format. Please extract the capital structure.\n\n{document_text}"},
                ],
                timeout=120.0,
            )
        resp = await run_in_threadpool(_do_call)
        content = resp.choices[0].message.content
        if not content: raise HTTPException(status_code=502, detail="AI service returned an empty response.")
        return json.loads(content)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse LLM JSON. Content: {content[:500]}")
        raise HTTPException(status_code=502, detail="AI service returned malformed JSON.")
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
    encoded_len = len(request.file_content)
    if (encoded_len * 3 / 4) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large.")
    try:
        file_bytes = base64.b64decode(request.file_content, validate=True)
    except (ValueError, binascii.Error):
        raise HTTPException(status_code=400, detail="Invalid base64 content.")
    if len(file_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File size exceeds limit.")
    ext = request.file_name.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_EXTS:
        raise HTTPException(status_code=415, detail="Unsupported file type. Please upload an .xlsx file.")
    file_id = f"upload_{uuid.uuid4()}.{ext}"
    try:
        with tempfile.NamedTemporaryFile(delete=False, prefix="cap_struct_", suffix=f"_{file_id}", dir="/tmp") as tmp:
            tmp.write(file_bytes)
            path = tmp.name
        file_storage[file_id] = {"path": path, "original_name": request.file_name}
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
        def _read_and_process():
            with open(path, "rb") as f:
                return process_and_anonymize_excel(f.read())
        
        document_text = await run_in_threadpool(_read_and_process)
        
        llm_obj = await call_llm(document_text)
        
        result = CapitalStructureInput.model_validate(llm_obj)
        if not result.securities:
            raise HTTPException(status_code=502, detail="AI returned a valid but empty list of securities.")
        return result
    except ValueError as e:
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
