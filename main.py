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

# --- App Config ---
PORT = int(os.getenv("PORT", "8080"))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", "25000000"))  # 25 MB
MAX_TEXT_FILE_CHARS = 2_000_000
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
    version="5.0.1-hotfix",
    description="API for extracting structured data from financial documents."
)

# --- CORS Middleware (Corrected for Development) ---
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
# 5. Helper Functions (Excel & Numeric Parsing)
# =============================================================================

NUMERIC_RE = re.compile(r"[-+]?(\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?")
NEG_PARENS_RE = re.compile(r"^\(\s*(.+?)\s*\)$")

def parse_numeric(cell: Any) -> Optional[float]:
    if cell is None: return None
    if isinstance(cell, (int, float)): return float(cell)
    s = str(cell).strip()
    if not s: return None
    if mneg := NEG_PARENS_RE.match(s):
        inner = mneg.group(1).replace("$", "").replace("€", "").replace("£", "")
        if m := NUMERIC_RE.search(inner):
            try: return -float(m.group(0).replace(",", ""))
            except (ValueError, TypeError): return None
    s_clean = s.replace("$", "").replace("€", "").replace("£", "")
    if m := NUMERIC_RE.search(s_clean):
        try: return float(m.group(0).replace(",", ""))
        except (ValueError, TypeError): return None
    return None

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

# RESTORED FUNCTION: This function was accidentally deleted in the previous version.
def extract_cap_table_structure(sheet) -> Dict[str, Any]:
    data = {"headers": [], "raw_headers": [], "totals": {}, "prices": {}, "options_outstanding": None, "option_pool_available": None}
    all_rows = list(sheet.iter_rows(values_only=True))
    if not all_rows: return data
    header_row_idx = None
    for idx, row in enumerate(all_rows[:30]):
        tokens = " ".join([str(c or "") for c in row]).lower()
        if sum(tok in tokens for tok in ["common", "preferred", "series", "price", "shares", "stock", "ownership"]) >= 2:
            header_row_idx, data["raw_headers"] = idx, list(row)
            cleaned_headers, skip_next = [], False
            for i, header in enumerate(row):
                if skip_next:
                    cleaned_headers.append(None)
                    skip_next = False
                    continue
                if header and is_conversion_ratio_column(str(header)):
                    cleaned_headers.append(None)
                elif header:
                    clean_name = clean_security_name(str(header))
                    cleaned_headers.append(clean_name if clean_name else None)
                    if i + 1 < len(row) and row[i + 1]:
                        next_header = str(row[i + 1])
                        if is_conversion_ratio_column(next_header) and clean_name and clean_name in next_header:
                            skip_next = True
                else:
                    cleaned_headers.append(None)
            data["headers"] = cleaned_headers
            break
    if header_row_idx is None: return data
    start_search = max(header_row_idx + 1, len(all_rows) - 30)
    TOTALS_ALIASES = ["fully diluted shares", "total shares outstanding", "fully-diluted shares", "fd shares", "outstanding shares", "basic shares"]
    PRICES_ALIAS = "price per share"
    OPTS_OUT_ALIASES = ["options and rsu's issued and outstanding", "options outstanding", "options issued and outstanding", "rsus outstanding"]
    POOL_AVAIL_ALIASES = ["available for issuance", "shares available", "unallocated", "remaining available"]
    def label_of(row_vals):
        if not row_vals: return None
        for i in (0, 1):
            if i < len(row_vals) and row_vals[i]: return str(row_vals[i]).strip().lower()
    for idx in range(start_search, len(all_rows)):
        row_label = label_of(row := all_rows[idx])
        if not row or not row_label: continue
        if any(alias in row_label for alias in TOTALS_ALIASES):
            for col_idx in range(1, min(len(row), len(data["headers"]))):
                if (header := data["headers"][col_idx]) and (val := parse_numeric(row[col_idx])) is not None and val >= 0: data["totals"][header] = float(val)
        elif PRICES_ALIAS in row_label:
            for col_idx in range(1, min(len(row), len(data["headers"]))):
                if (header := data["headers"][col_idx]) and (price := parse_numeric(row[col_idx])) is not None and price >= 0: data["prices"][header] = float(price)
        elif any(alias in row_label for alias in OPTS_OUT_ALIASES) and "outstanding" in row_label:
            for cell_value in row[1:]:
                if (val := parse_numeric(cell_value)) is not None and val > 0: data["options_outstanding"] = float(val); break
        elif any(alias in row_label for alias in POOL_AVAIL_ALIASES) and ("plan" in row_label or "issuance" in row_label) or "available for issuance under the plan" in row_label:
            for cell_value in row[1:]:
                if (val := parse_numeric(cell_value)) is not None and val > 0: data["option_pool_available"] = float(val); break
    return data

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
            entry = {"options_outstanding": float(out_val), "exercise_price": float(px_val)}
            if "name" in col_map: entry["name"] = row[col_map["name"]]
            options.append(entry)
        except (IndexError, KeyError, TypeError, ValueError): continue
    return options

def parse_excel_cap_table(file_bytes: bytes) -> Dict[str, Any]:
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
        cap_table_sheet, option_ledger_sheet = pick_candidate_sheets(workbook)
        logger.info(f"Selected sheets: cap_table='{cap_table_sheet.title}', options='{getattr(option_ledger_sheet, 'title', 'N/A')}'")
        cap_table_data = extract_cap_table_structure(cap_table_sheet)
        option_data = extract_option_ledger(option_ledger_sheet)
        return {"cap_table": cap_table_data, "option_ledger": option_data, "source": "excel"}
    except Exception as e:
        logger.error(f"Failed during Excel parsing stage: {e}")
        raise ValueError(f"Could not parse the provided Excel file. Error: {e}")


# =============================================================================
# 6. LLM Integration
# =============================================================================

EXTRACTION_SYSTEM_PROMPT = """You are an expert financial analyst specializing in venture capital cap tables.
You receive STRUCTURED data extracted from an Excel file.

INSTRUCTIONS:
1.  Process `cap_table.totals`: Create a security for every key in this dictionary.
    - **CRITICAL EXCEPTION**: If a key contains "Option", "RSU", or "Plan", **you MUST IGNORE it**. Do not create a general security for options from here. Options are handled ONLY in the next step.
    - For all other keys (e.g., "Common", "Series A Preferred Stock"):
        - `name` = the key
        - `shares_outstanding` = the value from `totals`
        - `original_investment_per_share` = the value from `prices` for that key, or 0.0 if missing.
        - Set other financial properties based on the security type (preferred vs. common).

2.  Process `option_ledger`: This is the **ONLY** source for options data. You MUST process every entry.
    - First, group all entries in the `option_ledger` list by their `exercise_price`.
    - For each unique exercise price group, you **MUST** create a new, separate security.
    - The `name` for each security **MUST** be "Options at $X.XX Exercise Price", where X.XX is the exercise price formatted to two decimal places.
    - The `shares_outstanding` for this security is the SUM of `options_outstanding` for all items in that group.
    - All other fields (`liquidation_preference_multiple`, `seniority`, etc.) for these option securities **MUST** be 0.0 or null as specified in the output schema.

3.  Set `total_option_pool_shares`:
    - This value is `cap_table.option_pool_available`, or 0 if it's missing.

4.  Final Output Rules:
    - **DO NOT** create a single, aggregated security like "Options and RSU's Outstanding". You MUST break them down by exercise price from the `option_ledger`.
    - Return ONLY the final JSON object. No markdown, no comments, no conversational text.
"""

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
            return client.chat.completions.create(
                model="gpt-4o-mini", response_format={"type": "json_object"},
                temperature=0.1, max_tokens=2048,
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Extract capital structure:\n\n{document_text}"},
                ],
                timeout=45.0,
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
        else:
            def _read_text():
                with open(path, "rb") as f:
                    text = f.read().decode("utf-8-sig", errors="replace")
                    return text[:MAX_TEXT_FILE_CHARS]
            document_text = await run_in_threadpool(_read_text)
        llm_obj = await call_llm(document_text)
        result = CapitalStructureInput.model_validate(llm_obj)
        if not result.securities:
            raise HTTPException(status_code=502, detail="AI service returned a valid but empty list of securities.")
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
