import os
import base64
import uuid
import json
import io
import re
import tempfile
import logging
from typing import List, Optional, Dict, Any, Tuple

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field, NonNegativeFloat, field_validator
from openai import OpenAI
import openpyxl

# =============================================================================
# Logging & Configuration
# =============================================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("capital-structure")

# --- App Config --------------------------------------------------------------
PORT = int(os.getenv("PORT", "8080"))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", "25000000"))  # 25 MB
ALLOWED_EXTS = {"xlsx", "txt", "csv"}

# --- OpenAI Client (v1+) -----------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.error("FATAL: OPENAI_API_KEY environment variable is not set.")
    # In a real app, you might want to exit here or handle it gracefully.
client = OpenAI(api_key=OPENAI_API_KEY)


# =============================================================================
# FastAPI App & CORS Middleware (DEVELOPMENT ONLY - PERMISSIVE)
# =============================================================================

app = FastAPI(
    title="Capital Structure API",
    version="3.1.0-fix", # Updated version
    description="API for extracting structured data from financial documents."
)

# WARNING: This is a permissive configuration for early development.
# It allows requests from ANY origin. This is a quick way to unblock development.
# Before moving to production, you MUST restrict this to your frontend's domain.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# In-memory index for temporary file paths
# =============================================================================

# TODO: This storage is ephemeral and will be wiped on server restart/deploy.
# For production, replace this with a persistent solution like a database record
# that points to a file in cloud object storage (e.g., AWS S3, Cloudflare R2).
file_storage: Dict[str, Dict[str, Any]] = {}


# =============================================================================
# Pydantic Models (unchanged)
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
        if not v or not v.strip():
            raise ValueError("name required")
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
# Helper Functions (unchanged from original)
# =============================================================================

NUMERIC_RE = re.compile(r"[-+]?(\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?")
NEG_PARENS_RE = re.compile(r"^\(\s*(.+?)\s*\)$")

def parse_numeric(cell: Any) -> Optional[float]:
    if cell is None:
        return None
    if isinstance(cell, (int, float)):
        return float(cell)
    s = str(cell).strip()
    if not s:
        return None
    mneg = NEG_PARENS_RE.match(s)
    if mneg:
        inner = mneg.group(1)
        m = NUMERIC_RE.search(inner.replace("$", "").replace("€", "").replace("£", ""))
        if m:
            try:
                return -float(m.group(0).replace(",", ""))
            except (ValueError, TypeError):
                return None
        return None
    s_clean = s.replace("$", "").replace("€", "").replace("£", "")
    m = NUMERIC_RE.search(s_clean)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except (ValueError, TypeError):
        return None

# ... Other parsing helpers like clean_security_name, extract_cap_table_structure, etc. remain the same ...
def clean_security_name(name: str) -> str:
    if not name:
        return ""
    cleaned = re.sub(r"\s*\([^)]*\)\s*", " ", str(name))
    cleaned = re.sub(r"\s*\d+\s*:\s*\d+\s*Conversion\s*Ratio", "", cleaned, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", cleaned).strip()


def is_conversion_ratio_column(header: str) -> bool:
    if not header:
        return False
    header_lower = str(header).lower()
    return bool(re.search(r"\bconversion\s*ratio\b|\b\d+\s*:\s*\d+\b", header_lower))


def pick_candidate_sheets(workbook) -> Tuple[Any, Optional[Any]]:
    cap_tokens = {"cap table", "detailed", "preferred", "series", "common", "stock"}
    opt_tokens = {"option", "grant", "ledger", "rsu"}
    best_cap, best_cap_score, opt_sheet = None, -1, None
    for sheet in workbook.worksheets:
        sheet_name_lower = sheet.title.lower()
        first_rows_text = " ".join([
            str(cell or "")
            for row in sheet.iter_rows(min_row=1, max_row=5, values_only=True)
            for cell in row
        ]).lower()
        cap_score = sum(tok in sheet_name_lower for tok in cap_tokens) + sum(tok in first_rows_text for tok in cap_tokens)
        opt_score = sum(tok in sheet_name_lower for tok in opt_tokens) + sum(tok in first_rows_text for tok in opt_tokens)
        if cap_score > best_cap_score:
            best_cap_score, best_cap = cap_score, sheet
        if opt_score > 0 and opt_sheet is None:
            opt_sheet = sheet
    return best_cap or workbook.worksheets[0], opt_sheet


def extract_cap_table_structure(sheet) -> Dict[str, Any]:
    # This extensive function remains the same
    data = {"headers": [],"raw_headers": [],"totals": {},"prices": {},"options_outstanding": None,"option_pool_available": None}
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
                    cleaned_headers.append(None); skip_next = False; continue
                if header and is_conversion_ratio_column(str(header)):
                    cleaned_headers.append(None)
                elif header:
                    clean_name = clean_security_name(str(header))
                    cleaned_headers.append(clean_name if clean_name else None)
                    if i + 1 < len(row) and row[i + 1]:
                        next_header = str(row[i + 1])
                        if is_conversion_ratio_column(next_header) and clean_name and clean_name in next_header:
                            skip_next = True
                else: cleaned_headers.append(None)
            data["headers"] = cleaned_headers; break
    if header_row_idx is None: return data
    start_search = max(header_row_idx + 1, len(all_rows) - 30)
    TOTALS_ALIASES = ["fully diluted shares","total shares outstanding","fully-diluted shares","fd shares","outstanding shares","basic shares"]
    PRICES_ALIAS = "price per share"
    OPTS_OUT_ALIASES = ["options and rsu's issued and outstanding","options outstanding","options issued and outstanding","rsus outstanding"]
    POOL_AVAIL_ALIASES = ["available for issuance","shares available","unallocated","remaining available"]
    def label_of(row_vals):
        if not row_vals: return None
        for i in (0, 1):
            if i < len(row_vals) and row_vals[i]: return str(row_vals[i]).strip().lower()
        return None
    for idx in range(start_search, len(all_rows)):
        row, row_label = all_rows[idx], label_of(all_rows[idx])
        if not row or not row_label: continue
        if any(alias in row_label for alias in TOTALS_ALIASES):
            for col_idx in range(1, min(len(row), len(data["headers"]))):
                header = data["headers"][col_idx]
                if header is not None and (val := parse_numeric(row[col_idx])) is not None and val >= 0:
                    data["totals"][header] = float(val)
        elif PRICES_ALIAS in row_label:
            for col_idx in range(1, min(len(row), len(data["headers"]))):
                header = data["headers"][col_idx]
                if header is not None and (price := parse_numeric(row[col_idx])) is not None and price >= 0:
                    data["prices"][header] = float(price)
        elif any(alias in row_label for alias in OPTS_OUT_ALIASES) and "outstanding" in row_label:
            for cell_value in row[1:]:
                if (val := parse_numeric(cell_value)) is not None and val > 0:
                    data["options_outstanding"] = float(val); break
        elif (any(alias in row_label for alias in POOL_AVAIL_ALIASES) and ("plan" in row_label or "issuance" in row_label)) or "available for issuance under the plan" in row_label:
            for cell_value in row[1:]:
                if (val := parse_numeric(cell_value)) is not None and val > 0:
                    data["option_pool_available"] = float(val); break
    return data


def extract_option_ledger(sheet) -> List[Dict[str, Any]]:
    # This extensive function remains the same
    if sheet is None: return []
    options: List[Dict[str, Any]] = []
    header_row_idx, col_map = None, {}
    synonyms = {"exercise_price": ["exercise price", "strike", "strike price", "price"],"options_outstanding": ["outstanding", "balance", "granted - outstanding", "options"],"id": ["id", "stakeholder id", "emp id"],"name": ["name", "optionholder", "holder", "employee"]}
    def best_key(col_name: str) -> Optional[str]:
        cl = col_name.lower().strip()
        for k, alts in synonyms.items():
            if any(a in cl for a in alts): return k
        return None
    for idx, row in enumerate(sheet.iter_rows(min_row=1, max_row=20, values_only=True)):
        tokens = " ".join([str(c or "").lower() for c in row])
        if any(w in tokens for w in ["exercise", "strike", "price"]) and any(w in tokens for w in ["option", "outstanding", "balance"]):
            header_row_idx = idx + 1
            for col_idx, cell in enumerate(row):
                if cell and (key := best_key(str(cell))) and key not in col_map:
                    col_map[key] = col_idx
            break
    if not header_row_idx or "options_outstanding" not in col_map or "exercise_price" not in col_map: return []
    for row in sheet.iter_rows(min_row=header_row_idx + 1, values_only=True):
        try:
            outstanding = row[col_map["options_outstanding"]] if col_map["options_outstanding"] < len(row) else None
            exercise_price = row[col_map["exercise_price"]] if col_map["exercise_price"] < len(row) else None
            out_val, px_val = parse_numeric(outstanding), parse_numeric(exercise_price)
            if out_val is None or px_val is None or out_val <= 0: continue
            entry = {"options_outstanding": float(out_val),"exercise_price": float(px_val)}
            if "id" in col_map and col_map["id"] < len(row): entry["id"] = row[col_map["id"]]
            if "name" in col_map and col_map["name"] < len(row): entry["name"] = row[col_map["name"]]
            options.append(entry)
        except Exception: continue
    return options


def parse_excel_cap_table(file_bytes: bytes) -> Dict[str, Any]:
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
        cap_table_sheet, option_ledger_sheet = pick_candidate_sheets(workbook)
        cap_table_data = extract_cap_table_structure(cap_table_sheet)
        option_data = extract_option_ledger(option_ledger_sheet)
        return {"cap_table": cap_table_data, "option_ledger": option_data, "source": "excel"}
    except Exception as e:
        logger.error(f"Failed to parse Excel file: {e}")
        # Re-raising is good, but let's make it an HTTPException for the client.
        raise HTTPException(status_code=400, detail=f"Could not parse Excel file: {e}")


# =============================================================================
# LLM Integration
# =============================================================================

# IMPROVED: This prompt is more explicit and forceful to ensure the AI correctly
# processes options by breaking them down by exercise price.
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
    """Tolerantly repair common model mistakes before Pydantic validation."""
    if not isinstance(obj, dict):
        return obj
    if "securities" in obj and isinstance(obj["securities"], list):
        for item in obj["securities"]:
            if isinstance(item, dict) and "name" not in item and "security" in item:
                item["name"] = item.pop("security")
    return obj


async def call_llm(document_text: str) -> Dict[str, Any]:
    """Modern, async-friendly OpenAI API call using the v1+ client."""
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="Server misconfiguration: OpenAI API key missing")

    try:
        # run_in_threadpool is used because the OpenAI client's methods are synchronous
        def _do_call():
            return client.chat.completions.create(
                model="gpt-4o-mini",
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=2048,  # Increased for potentially larger cap tables
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Extract capital structure:\n\n{document_text}"},
                ],
                timeout=45.0,  # Seconds
            )
        resp = await run_in_threadpool(_do_call)
        content = resp.choices[0].message.content

    except Exception as e:
        logger.error(f"OpenAI API call failed: {e}")
        raise HTTPException(status_code=503, detail="AI service is currently unavailable.")

    if not content:
        raise HTTPException(status_code=502, detail="AI service returned an empty response.")

    try:
        parsed = json.loads(content)
        return _remap_common_llm_mistakes(parsed)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse LLM JSON response. Content: {content[:500]}")
        raise HTTPException(status_code=502, detail="Failed to parse AI response.")


# =============================================================================
# API Routes
# =============================================================================

@app.get("/", summary="API Root", tags=["Health"])
async def root():
    return {"message": "Capital Structure API", "version": app.version}


@app.get("/health", summary="Health Check", tags=["Health"])
async def health():
    return {"status": "healthy"}


@app.post("/api/documents/upload", response_model=DocumentUploadResponse, status_code=201, tags=["Document Processing"])
async def upload_document(request: FileUploadRequest):
    """Accepts a base64-encoded file and stores it for processing."""
    if "." not in request.file_name:
        raise HTTPException(status_code=400, detail="Filename must have an extension.")

    ext = request.file_name.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_EXTS:
        detail = "Legacy .xls not supported. Please save and upload as .xlsx." if ext == "xls" else f"Unsupported file type: .{ext}"
        raise HTTPException(status_code=415, detail=detail)

    try:
        file_bytes = base64.b64decode(request.file_content)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid base64 content in payload.")

    if len(file_bytes) == 0:
        raise HTTPException(status_code=400, detail="Cannot process an empty file.")
    if len(file_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File size exceeds limit of {MAX_UPLOAD_BYTES / 1_000_000} MB.")

    file_id = f"upload_{uuid.uuid4()}.{ext}"

    try:
        # Using a context manager for cleaner temporary file handling
        with tempfile.NamedTemporaryFile(delete=False, prefix="cap_struct_", suffix=f"_{file_id}", dir="/tmp") as tmp:
            tmp.write(file_bytes)
            path = tmp.name

        file_storage[file_id] = {
            "path": path,
            "original_name": request.file_name,
            "size": len(file_bytes),
            "extension": ext,
        }
        return DocumentUploadResponse(
            file_id=file_id,
            file_name=request.file_name,
            message="File uploaded successfully",
            file_size_bytes=len(file_bytes),
        )
    except Exception as e:
        logger.exception(f"Failed to write temporary file for upload: {e}")
        raise HTTPException(status_code=500, detail="Failed to save uploaded file on server.")


@app.post("/api/documents/extract", response_model=CapitalStructureInput, tags=["Document Processing"])
async def extract_data(payload: DocumentExtractRequest):
    """Extracts structured data from a previously uploaded file."""
    rid = str(uuid.uuid4())[:8] # Request ID for logging
    logger.info(f"[rid={rid}] Extraction requested for file_id={payload.file_id}")

    if payload.file_id not in file_storage:
        raise HTTPException(status_code=404, detail="File not found. It may have expired or never been uploaded.")

    meta = file_storage[payload.file_id]
    path = meta.get("path")
    ext = meta.get("extension", "txt")

    if not path or not os.path.exists(path):
        logger.error(f"[rid={rid}] File metadata found, but file does not exist at path: {path}")
        raise HTTPException(status_code=410, detail="File has expired and been cleaned up from the server.")

    document_text = ""
    try:
        if ext == "xlsx":
            logger.info(f"[rid={rid}] Parsing Excel file: {meta['original_name']}")
            def _read_and_parse():
                with open(path, "rb") as f:
                    file_bytes = f.read()
                return parse_excel_cap_table(file_bytes)

            structured_data = await run_in_threadpool(_read_and_parse)
            document_text = json.dumps(structured_data, separators=(",", ":"))
        else: # Handle .txt, .csv, etc. as plain text
            logger.info(f"[rid={rid}] Reading as text file: {meta['original_name']}")
            def _read_text():
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    return f.read()
            document_text = await run_in_threadpool(_read_text)

        # Call the LLM with the processed text
        llm_obj = await call_llm(document_text)

        # Validate the response from the LLM against our Pydantic model
        result = CapitalStructureInput.model_validate(llm_obj)
        return result

    except HTTPException:
        # Re-raise HTTPExceptions from helpers (e.g., call_llm) directly
        raise
    except Exception as e:
        logger.exception(f"[rid={rid}] Unexpected error during data extraction: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred during extraction.")
    finally:
        # --- IMPORTANT: Clean up the temporary file ---
        # This runs whether the extraction succeeds or fails.
        try:
            if path and os.path.exists(path):
                os.remove(path)
                logger.info(f"[rid={rid}] Cleaned up temporary file: {path}")
            # Also remove the entry from our in-memory dictionary
            if payload.file_id in file_storage:
                del file_storage[payload.file_id]
        except Exception as e:
            logger.error(f"[rid={rid}] Failed to clean up temporary file {path}: {e}")

# =============================================================================
# Server Entrypoint
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
