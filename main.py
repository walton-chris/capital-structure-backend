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
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s [%(name)s] - %(message)s")
logger = logging.getLogger("capital-structure")

PORT = int(os.getenv("PORT", "8080"))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", "25000000"))
ALLOWED_EXTS = {"xlsx"}

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client: Optional[OpenAI] = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


# =============================================================================
# 2. FastAPI App & Models
# =============================================================================
app = FastAPI(
    title="Capital Structure API",
    version="9.1.0-refined-hybrid",
    description="API using a refined hybrid model for fast and accurate extraction."
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "https://capital-structure-frontend.vercel.app"],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

file_storage: Dict[str, Dict[str, Any]] = {}

class Security(BaseModel):
    name: str; shares_outstanding: NonNegativeFloat; original_investment_per_share: NonNegativeFloat
    liquidation_preference_multiple: NonNegativeFloat; seniority: Optional[int] = Field(default=None, ge=0, le=10)
    is_participating: bool; participation_cap_multiple: NonNegativeFloat; cumulative_dividend_rate: NonNegativeFloat
    years_since_issuance: NonNegativeFloat
    @field_validator("name")
    def _non_empty(cls, v: str) -> str:
        if not v or not v.strip(): raise ValueError("Security 'name' cannot be empty.")
        return v.strip()
class CapitalStructureInput(BaseModel): securities: List[Security]; total_option_pool_shares: NonNegativeFloat
class FileUploadRequest(BaseModel): file_content: str; file_name: str
class DocumentExtractRequest(BaseModel): file_id: str
class DocumentUploadResponse(BaseModel): file_id: str; file_name: str; message: str; file_size_bytes: int


# =============================================================================
# 3. Hybrid Approach: Pre-Filter, Anonymize, and Convert
# =============================================================================
class Anonymizer:
    def __init__(self):
        self.name_map: Dict[str, str] = {}; self.person_counter = 1; self.entity_counter = 1
        self.entity_pattern = re.compile(r'(LLC|Inc|LP|FBO|Capital|Partners|Fund|Trust|Ventures)', re.IGNORECASE)
    def anonymize_name(self, name: str) -> str:
        if not name or not isinstance(name, str): return name
        name = name.strip()
        if name in self.name_map: return self.name_map[name]
        is_entity = self.entity_pattern.search(name) or name.isupper()
        placeholder = f"Entity-{self.entity_counter}" if is_entity else f"Person-{self.person_counter}"
        if is_entity: self.entity_counter += 1
        else: self.person_counter += 1
        self.name_map[name] = placeholder
        return placeholder

def process_excel_for_llm(file_bytes: bytes, rid: str) -> str:
    """Efficiently pre-filters, anonymizes, and converts only relevant Excel sheets to Markdown."""
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
        anonymizer = Anonymizer()
        
        cap_sheet, option_sheet = None, None
        for sheet in workbook.worksheets:
            title = sheet.title.lower()
            if not cap_sheet and "detailed cap" in title: cap_sheet = sheet
            if not option_sheet and ("option" in title or "grant" in title): option_sheet = sheet
        
        if not cap_sheet:
            logger.warning(f"[rid={rid}] Could not find 'Detailed Cap Table' sheet, falling back to first sheet.")
            cap_sheet = workbook.worksheets[0]

        sheets_to_process = [s for s in [cap_sheet, option_sheet] if s is not None]
        logger.info(f"[rid={rid}] Pre-selected sheets for processing: {[s.title for s in sheets_to_process]}")

        markdown_parts = []
        for sheet in sheets_to_process:
            markdown_parts.append(f"## Sheet: {sheet.title}\n")
            # Anonymize stakeholder columns before converting to markdown
            headers = [str(cell.value) if cell.value is not None else "" for cell in sheet[1]]
            stakeholder_col_idx = -1
            for i, h in enumerate(headers):
                if h.lower() in ["stakeholder", "optionholder", "name"]:
                    stakeholder_col_idx = i
                    break
            
            rows = []
            for i, row in enumerate(sheet.iter_rows(values_only=True)):
                if not any(cell is not None for cell in row): continue # Skip empty rows
                
                row_list = list(row)
                if stakeholder_col_idx != -1 and i > 0 and len(row_list) > stakeholder_col_idx: # i>0 to skip header
                     row_list[stakeholder_col_idx] = anonymizer.anonymize_name(str(row_list[stakeholder_col_idx]))
                
                rows.append([str(cell) if cell is not None else "" for cell in row_list])

            if not rows: continue
            
            header_row = rows[0]
            separator = ["---" for _ in header_row]
            markdown_parts.append(f"| {' | '.join(header_row)} |")
            markdown_parts.append(f"| {' | '.join(separator)} |")
            for row in rows[1:]:
                if len(row) < len(header_row): row.extend([""] * (len(header_row) - len(row)))
                markdown_parts.append(f"| {' | '.join(row[:len(header_row)])} |")
            markdown_parts.append("\n")
            
        return "\n".join(markdown_parts)
    except Exception as e:
        logger.exception(f"[rid={rid}] Failed during hybrid Excel processing.")
        raise ValueError(f"Could not parse, filter, and anonymize the Excel file. Error: {e}")

# =============================================================================
# 4. LLM Integration (Single, Powerful Call with Refined Prompt)
# =============================================================================

# REFINED PROMPT: This prompt is highly specific about how to parse the different table layouts.
EXTRACTION_PROMPT = """You are an expert financial analyst. You will be given pre-filtered, anonymized data from a company's key cap table sheets in Markdown format.

Your task is to meticulously analyze the provided tables and convert the data into a single, structured JSON object.

**INSTRUCTIONS FOR PARSING**:

1.  **Analyze the "Detailed Cap Table" Sheet**:
    * This table is **pivoted**. The security names (e.g., "Common Stock", "Series A Preferred Stock") are the **COLUMN HEADERS**.
    * To find the shares for each security, you **MUST** locate the row where the first column is 'Total Shares Outstanding'.
    * Read the values **horizontally** across this 'Total Shares Outstanding' row. Each value corresponds to the security in its respective column header.

2.  **Analyze the "Option Plan" Sheet**:
    * In this table, each **ROW** represents an option grant or a group of grants.
    * Identify the columns for 'Shares' (or 'Amount') and 'Exercise Price' (or 'Strike Price').
    * You **MUST** group all rows by their unique **Exercise Price** and sum the 'Shares' for each group.

**CRITICAL OUTPUT RULES**:
- Create a **separate** security entry in the final JSON for each distinct option group. The name must be "Options at $X.XX Exercise Price".
- **DO NOT** create a single, aggregated security for all options from the main cap table.
- Ignore any rows/columns that are clearly totals or summaries, unless specified above.
- Your final output must be a single, valid JSON object and nothing else.
"""

async def call_llm_single_shot(focused_markdown: str, rid: str) -> Dict[str, Any]:
    if client is None: raise HTTPException(status_code=503, detail="AI service is not configured.")
    logger.info(f"[rid={rid}] Starting single-shot AI extraction with refined prompt.")
    try:
        def _do_call():
            return client.chat.completions.create(
                model="gpt-4o", temperature=0.0,
                response_format={"type": "json_object"},
                max_tokens=4096,
                messages=[{"role": "system", "content": EXTRACTION_PROMPT}, {"role": "user", "content": focused_markdown}],
                timeout=120.0,
            )
        resp = await run_in_threadpool(_do_call)
        content = resp.choices[0].message.content
        if not content: raise HTTPException(status_code=502, detail="AI returned an empty response.")
        logger.info(f"[rid={rid}] AI extraction complete.")
        return json.loads(content)
    except json.JSONDecodeError:
        logger.error(f"[rid={rid}] Failed to parse LLM JSON. Content: {content[:500]}")
        raise HTTPException(status_code=502, detail="AI returned malformed JSON.")
    except Exception as e:
        logger.error(f"[rid={rid}] AI call failed: {e}")
        raise HTTPException(status_code=503, detail="AI service is unavailable or timed out.")


# =============================================================================
# 5. API Routes & Server Entrypoint
# =============================================================================

@app.get("/", include_in_schema=False)
async def root(): return {"message": "Capital Structure API", "version": app.version}

@app.get("/health", tags=["Health"])
async def health(): return {"status": "healthy"}

@app.post("/api/documents/upload", response_model=DocumentUploadResponse, status_code=201, tags=["Processing"])
async def upload_document(request: FileUploadRequest):
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

@app.post("/api/documents/extract", response_model=CapitalStructureInput, tags=["Processing"])
async def extract_data(payload: DocumentExtractRequest):
    rid = str(uuid.uuid4())[:8]
    logger.info(f"[rid={rid}] Extraction requested for file_id={payload.file_id}")
    if payload.file_id not in file_storage: raise HTTPException(status_code=404, detail="File not found.")
    path = file_storage[payload.file_id].get("path")
    if not path or not os.path.exists(path):
        logger.error(f"[rid={rid}] File missing at path: {path}")
        raise HTTPException(status_code=410, detail="File has expired.")
    
    llm_obj = None
    try:
        def _read_and_process():
            with open(path, "rb") as f:
                return process_excel_for_llm(f.read(), rid)
        
        focused_markdown = await run_in_threadpool(_read_and_process)
        llm_obj = await call_llm_single_shot(focused_markdown, rid)
        result = CapitalStructureInput.model_validate(llm_obj)
        if not result.securities:
            raise HTTPException(status_code=502, detail="AI returned a valid but empty list of securities.")
        return result
    except ValidationError as e:
        error_details = e.errors()
        logger.error(f"[rid={rid}] Final JSON failed validation. Details: {error_details}. AI Response: {llm_obj}")
        first_error = error_details[0]
        field = " -> ".join(map(str, first_error['loc']))
        msg = first_error['msg']
        raise HTTPException(status_code=502, detail=f"AI returned invalid data structure. Field: '{field}', Error: {msg}")
    except (ValueError, HTTPException) as e:
        status_code = e.status_code if isinstance(e, HTTPException) else 400
        detail = e.detail if isinstance(e, HTTPException) else str(e)
        logger.warning(f"[rid={rid}] Handled error during extraction: {detail}")
        raise HTTPException(status_code=status_code, detail=detail)
    except Exception:
        logger.exception(f"[rid={rid}] An unexpected, unhandled error occurred.")
        raise HTTPException(status_code=500, detail="An unexpected server error occurred.")
    finally:
        try:
            if path and os.path.exists(path):
                os.remove(path); logger.info(f"[rid={rid}] Cleaned up temp file: {path}")
            if payload.file_id in file_storage:
                del file_storage[payload.file_id]
        except Exception as e:
            logger.error(f"[rid={rid}] CRITICAL: Failed to clean up temp file {path}: {e}")

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting server on port {PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
