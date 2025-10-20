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

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field, NonNegativeFloat, field_validator, ValidationError
from openai import OpenAI

# =============================================================================
# 1. Logging & Configuration
# =============================================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d] - %(message)s",
)
logger = logging.getLogger("capital-structure")

# --- App Config ---
PORT = int(os.getenv("PORT", "8080"))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", "25000000"))  # 25 MB
MAX_TEXT_FILE_CHARS = 2_000_000 # Limit characters sent to LLM for text files
ALLOWED_EXTS = {"xlsx", "txt", "csv"}

# --- OpenAI Client (v1+) ---
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
    version="5.0.0-hardened",
    description="API for extracting structured data from financial documents."
)

# --- CORS Middleware (Corrected for Development) ---
# This configuration is now VALID and allows credentials from specific origins.
# It fixes the browser preflight error.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173", # Vite default
        "http://localhost:3000", # Create React App default
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
# 5. Helper Functions (Excel & Numeric Parsing)
# =============================================================================

NUMERIC_RE = re.compile(r"[-+]?(\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?")
NEG_PARENS_RE = re.compile(r"^\(\s*(.+?)\s*\)$")

def parse_numeric(cell: Any) -> Optional[float]:
    # TODO: Does not handle European-style decimals like '1.234,56'.
    if cell is None: return None
    if isinstance(cell, (int, float)): return float(cell)
    s = str(cell).strip()
    if not s: return None
    if mneg := NEG_PARENS_RE.match(s):
        inner = mneg.group(1).replace("$", "").replace("€", "").replace("£", "")
        if m := NUMERIC_RE.search(inner):
            try: return -float(m.group(0).replace(",", ""))
            except (ValueError, TypeError): return None
        return None
    s_clean = s.replace("$", "").replace("€", "").replace("£", "")
    if m := NUMERIC_RE.search(s_clean):
        try: return float(m.group(0).replace(",", ""))
        except (ValueError, TypeError): return None
    return None

# ... Other parsing helpers ...
def clean_security_name(name: str) -> str:
    if not name: return ""
    cleaned = re.sub(r"\s*\([^)]*\)\s*", " ", str(name))
    cleaned = re.sub(r"\s*\d+\s*:\s*\d+\s*Conversion\s*Ratio", "", cleaned, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", cleaned).strip()

def is_conversion_ratio_column(header: str) -> bool:
    if not header: return False
    return bool(re.search(r"\bconversion\s*ratio\b|\b\d+\s*:\s*\d+\b", str(header).lower()))

def pick_candidate_sheets(workbook) -> Tuple[Any, Optional[Any]]:
    cap_tokens = {"cap table", "detailed", "preferred", "series", "common", "stock"}
    opt_tokens = {"option", "grant", "ledger", "rsu"}
    best_cap, best_cap_score, opt_sheet = None, -1, None
    for sheet in workbook.worksheets:
        sheet_name_lower = sheet.title.lower()
        first_rows_text = " ".join(str(cell or "") for row in sheet.iter_rows(min_row=1, max_row=5, values_only=True) for cell in row).lower()
        cap_score = sum(tok in sheet_name_lower for tok in cap_tokens) + sum(tok in first_rows_text for tok in cap_tokens)
        opt_score = sum(tok in sheet_name_lower for tok in opt_tokens) + sum(tok in first_rows_text for tok in opt_tokens)
        if cap_score > best_cap_score:
            best_cap_score, best_cap = cap_score, sheet
        if opt_score > 0 and opt_sheet is None:
            opt_sheet = sheet
    return best_cap or workbook.worksheets[0], opt_sheet

def extract_option_ledger(sheet) -> List[Dict[str, Any]]:
    if sheet is None: return []
    options: List[Dict[str, Any]] = []
    header_row_idx, col_map = None, {}
    price_synonyms = ["exercise price", "strike", "price"]
    outstanding_synonyms = ["outstanding", "balance", "granted - outstanding", "options"]
    synonyms = {"exercise_price": price_synonyms, "options_outstanding": outstanding_synonyms, "name": ["name", "holder"]}
    def best_key(col_name: str) -> Optional[str]:
        cl = col_name.lower().strip()
        for k, alts in synonyms.items():
            if any(a in cl for a in alts): return k
    for idx, row in enumerate(sheet.iter_rows(min_row=1, max_row=20, values_only=True)):
        tokens = " ".join([str(c or "").lower() for c in row])
        # FIX: Broadened gate to be more flexible with header names.
        if any(w in tokens for w in price_synonyms) and any(w in tokens for w in outstanding_synonyms):
            header_row_idx = idx + 1
            for col_idx, cell in enumerate(row):
                if cell and (key := best_key(str(cell))) and key not in col_map: col_map[key] = col_idx
            break
    if not header_row_idx or "options_outstanding" not in col_map or "exercise_price" not in col_map: return []
    for row in sheet.iter_rows(min_row=header_row_idx + 1, values_only=True):
        try:
            out_val = parse_numeric(row[col_map["options_outstanding"]])
            px_val = parse_numeric(row[col_map["exercise_price"]])
            if out_val is None or px_val is None or out_val <= 0: continue
            entry = {"options_outstanding": float(out_val),"exercise_price": float(px_val)}
            if "name" in col_map: entry["name"] = row[col_map["name"]]
            options.append(entry)
        except (IndexError, KeyError, TypeError, ValueError): continue
    return options

def parse_excel_cap_table(file_bytes: bytes) -> Dict[str, Any]:
    # This function now raises ValueError instead of HTTPException.
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
        cap_table_sheet, option_ledger_sheet = pick_candidate_sheets(workbook)
        logger.info(f"Selected sheets: cap_table='{cap_table_sheet.title}', options='{getattr(option_ledger_sheet, 'title', 'N/A')}'")
        cap_table_data = extract_cap_table_structure(cap_table_sheet) # extract_cap_table_structure is not defined
        option_data = extract_option_ledger(option_ledger_sheet)
        return {"cap_table": cap_table_data, "option_ledger": option_data, "source": "excel"}
    except Exception as e:
        logger.error(f"Failed during Excel parsing stage: {e}")
        raise ValueError(f"Could not parse the provided Excel file. Error: {e}")


# =============================================================================
# 6. LLM Integration
# =============================================================================

EXTRACTION_SYSTEM_PROMPT = """You are an expert financial analyst...""" # Same improved prompt as before

def _remap_common_llm_mistakes(obj: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(obj, dict): return obj
    if "securities" in obj and isinstance(obj["securities"], list):
        for item in obj["securities"]:
            if isinstance(item, dict) and "name" not in item and "security" in item:
                item["name"] = item.pop("security")
    return obj

async def call_llm(document_text: str) -> Dict[str, Any]:
    if client is None:
        raise HTTPException(status_code=503, detail="AI service is not configured on the server.")
    try:
        def _do_call():
            # TODO: Consider httpx.Timeout(5.0, read=45.0) for granular control.
            return client.chat.completions.create(
                model="gpt-4o-mini", response_format={"type": "json_object"},
                temperature=0.1, max_tokens=2048,
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Extract capital structure:\n\n{document_text}"},
                ],
                timeout=45.0, # Timeout in seconds
            )
        resp = await run_in_threadpool(_do_call)
        content = resp.choices[0].message.content
    except Exception as e:
        logger.error(f"OpenAI API call failed: {e}")
        raise HTTPException(status_code=503, detail="AI service is unavailable or timed out.")
    if not content:
        raise HTTPException(status_code=502, detail="AI service returned an empty response.")
    try:
        parsed = json.loads(content)
        return _remap_common_llm_mistakes(parsed)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse LLM JSON. Content: {content[:500]}")
        raise HTTPException(status_code=502, detail="AI service returned a malformed response.")


# =============================================================================
# 7. API Routes
# =============================================================================

@app.get("/", summary="API Root", tags=["Health"])
async def root(): return {"message": "Capital Structure API", "version": app.version}

@app.get("/health", summary="Health Check", tags=["Health"])
async def health(): return {"status": "healthy"}

@app.post("/api/documents/upload", response_model=DocumentUploadResponse, status_code=201, tags=["Document Processing"])
async def upload_document(request: FileUploadRequest):
    # TODO: For idempotency, consider using a hash of file content+name for the file_id.
    # FIX: Check encoded size first to prevent large memory allocation on decode.
    encoded_len = len(request.file_content)
    if (encoded_len * 3 / 4) > MAX_UPLOAD_BYTES: # Base64 is ~4/3 larger
        raise HTTPException(status_code=413, detail="File too large.")
    try:
        # FIX: Use validate=True for strict Base64 decoding.
        file_bytes = base64.b64decode(request.file_content, validate=True)
    except (ValueError, binascii.Error):
        raise HTTPException(status_code=400, detail="Invalid base64 content in payload.")
    if len(file_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File size exceeds limit of {MAX_UPLOAD_BYTES / 1_000_000} MB.")
    # ... rest of the function is similar ...
    ext = request.file_name.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_EXTS:
        detail = "Legacy .xls not supported." if ext == "xls" else f"Unsupported file type: .{ext}"
        raise HTTPException(status_code=415, detail=detail)
    file_id = f"upload_{uuid.uuid4()}.{ext}"
    try:
        with tempfile.NamedTemporaryFile(delete=False, prefix="cap_struct_", suffix=f"_{file_id}", dir="/tmp") as tmp:
            tmp.write(file_bytes)
            path = tmp.name
        file_storage[file_id] = {"path": path, "original_name": request.file_name, "size": len(file_bytes), "extension": ext}
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
        ext = meta.get("extension", "txt")
        if ext == "xlsx":
            def _read_and_parse():
                with open(path, "rb") as f: return parse_excel_cap_table(f.read())
            structured_data = await run_in_threadpool(_read_and_parse)
            document_text = json.dumps(structured_data, separators=(",", ":"))
        else: # Handle text files
            def _read_text():
                with open(path, "rb") as f:
                    # FIX: Use utf-8-sig to strip BOM, truncate to prevent token bloat.
                    text = f.read().decode("utf-8-sig", errors="replace")
                    return text[:MAX_TEXT_FILE_CHARS]
            document_text = await run_in_threadpool(_read_text)
        llm_obj = await call_llm(document_text)
        result = CapitalStructureInput.model_validate(llm_obj)
        # FIX: Guard against valid but empty AI responses.
        if not result.securities:
            raise HTTPException(status_code=502, detail="AI service returned a valid but empty list of securities.")
        return result
    except ValueError as e: # Catch domain errors from helpers
        raise HTTPException(status_code=400, detail=str(e))
    except ValidationError as e: # FIX: Gracefully handle Pydantic validation errors
        # FIX: Log shapes/keys only, not the full (potentially sensitive) object
        shapes = [sorted(s.keys()) for s in (llm_obj.get("securities") or [])[:3] if isinstance(s, dict)]
        logger.error(f"[rid={rid}] AI response failed validation. Shapes: {shapes}, Errors: {e.errors()}")
        raise HTTPException(status_code=502, detail="AI response failed validation.")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[rid={rid}] Unexpected error during extraction.")
        raise HTTPException(status_code=500, detail="An unexpected server error occurred.")
    finally:
        # TODO: This cleanup prevents retries. For production, consider a timed
        # cleanup job instead of immediate deletion.
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
