import os
import base64
import uuid
import json
import io
import re
import tempfile
import logging
from typing import List, Optional, Dict, Any, Tuple
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field, NonNegativeFloat, field_validator

import openpyxl

# =============================================================================
# Logging
# =============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("capital-structure")

# =============================================================================
# Config
# =============================================================================

PORT = int(os.getenv("PORT", "8080"))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", "25000000"))  # 25 MB
ALLOWED_EXTS = {"xlsx", "txt", "csv"}  # Explicitly reject .xls

# --- OpenAI client (prefer v1) -----------------------------------------------

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.error("OPENAI_API_KEY is not set")

try:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    OPENAI_IS_V1 = True
except Exception:
    # Legacy fallback
    import openai  # type: ignore
    openai.api_key = OPENAI_API_KEY
    client = None  # type: ignore
    OPENAI_IS_V1 = False
    logger.warning("Using legacy openai.ChatCompletion client; consider upgrading to v1.")

# =============================================================================
# App & CORS
# =============================================================================

app = FastAPI(title="Capital Structure API", version="2.2.0")

# --- CORS config (fail-open for dev, safe for prod via env) ------------------
ALLOW_CREDENTIALS = os.getenv("CORS_ALLOW_CREDENTIALS", "true").lower() == "true"
ALLOWED_ORIGINS_ENV = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
ALLOWED_ORIGIN_REGEX = os.getenv("CORS_ALLOWED_ORIGIN_REGEX", "").strip()

allowed_origins = [o.strip() for o in ALLOWED_ORIGINS_ENV.split(",") if o.strip()]

# If credentials are on but no origins configured, avoid 400 preflights by
# disabling credentials and allowing '*' (useful during setup/local dev).
if ALLOW_CREDENTIALS and not allowed_origins and not ALLOWED_ORIGIN_REGEX:
    logging.warning(
        "CORS credentials enabled but no origins configured; "
        "disabling credentials and allowing '*'. Set CORS_ALLOWED_ORIGINS in prod."
    )
    ALLOW_CREDENTIALS = False
    allowed_origins = ["*"]

cors_kwargs = dict(
    allow_origins=allowed_origins if allowed_origins else ["*"],
    allow_credentials=ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=600,
)
if ALLOWED_ORIGIN_REGEX:
    cors_kwargs["allow_origin_regex"] = ALLOWED_ORIGIN_REGEX

app.add_middleware(CORSMiddleware, **cors_kwargs)

# =============================================================================
# In-memory index -> disk-backed file storage
# =============================================================================

file_storage: Dict[str, Dict[str, Any]] = {}  # stores pointers/metadata, not bytes


# =============================================================================
# Models
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
# Helpers: numeric parsing & Excel parsing
# =============================================================================

NUMERIC_RE = re.compile(r"[-+]?(\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?")
NEG_PARENS_RE = re.compile(r"^\(\s*(.+?)\s*\)$")


def parse_numeric(cell: Any) -> Optional[float]:
    """Coerce spreadsheet values to float: $, commas, parentheses, etc."""
    if cell is None:
        return None
    if isinstance(cell, (int, float)):
        return float(cell)
    s = str(cell).strip()
    if not s:
        return None
    # negatives like (123.45)
    mneg = NEG_PARENS_RE.match(s)
    if mneg:
        inner = mneg.group(1)
        m = NUMERIC_RE.search(inner.replace("$", "").replace("€", "").replace("£", ""))
        if m:
            try:
                return -float(m.group(0).replace(",", ""))
            except Exception:
                return None
        return None
    # normal path
    s_clean = s.replace("$", "").replace("€", "").replace("£", "")
    m = NUMERIC_RE.search(s_clean)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except Exception:
        return None


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
    """Score worksheets by token presence to pick cap table and option sheets."""
    cap_tokens = {"cap table", "detailed", "preferred", "series", "common", "stock"}
    opt_tokens = {"option", "grant", "ledger", "rsu"}

    best_cap = None
    best_cap_score = -1
    opt_sheet = None

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
            best_cap_score = cap_score
            best_cap = sheet
        if opt_score > 0 and opt_sheet is None:
            opt_sheet = sheet

    if best_cap is None:
        best_cap = workbook.worksheets[0]
    return best_cap, opt_sheet


def extract_cap_table_structure(sheet) -> Dict[str, Any]:
    data = {
        "headers": [],
        "raw_headers": [],
        "totals": {},
        "prices": {},
        "options_outstanding": None,
        "option_pool_available": None,
    }

    all_rows = list(sheet.iter_rows(values_only=True))
    if not all_rows:
        return data

    header_row_idx = None
    for idx, row in enumerate(all_rows[:30]):
        tokens = " ".join([str(c or "") for c in row]).lower()
        score = sum(tok in tokens for tok in ["common", "preferred", "series", "price", "shares", "stock", "ownership"])
        if score >= 2:
            header_row_idx = idx
            data["raw_headers"] = list(row)
            cleaned_headers = []
            skip_next = False
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

    if header_row_idx is None:
        return data

    start_search = max(header_row_idx + 1, len(all_rows) - 30)
    logger.debug("Scanning rows %s..%s for summary data", start_search, len(all_rows))

    TOTALS_ALIASES = [
        "fully diluted shares",
        "total shares outstanding",
        "fully-diluted shares",
        "fd shares",
        "outstanding shares",
        "basic shares",
    ]
    PRICES_ALIAS = "price per share"
    OPTS_OUT_ALIASES = [
        "options and rsu's issued and outstanding",
        "options outstanding",
        "options issued and outstanding",
        "rsus outstanding",
    ]
    POOL_AVAIL_ALIASES = [
        "available for issuance",
        "shares available",
        "unallocated",
        "remaining available",
    ]

    def label_of(row_vals) -> Optional[str]:
        if not row_vals:
            return None
        for i in (0, 1):
            if i < len(row_vals) and row_vals[i]:
                return str(row_vals[i]).strip().lower()
        return None

    for idx in range(start_search, len(all_rows)):
        row = all_rows[idx]
        if not row:
            continue
        row_label = label_of(row)
        if not row_label:
            continue

        # Totals
        if any(alias in row_label for alias in TOTALS_ALIASES):
            for col_idx in range(1, min(len(row), len(data["headers"]))):
                header = data["headers"][col_idx]
                if header is None:
                    continue
                val = parse_numeric(row[col_idx])
                if val is not None and val >= 0:
                    data["totals"][header] = float(val)

        # Prices
        elif PRICES_ALIAS in row_label:
            for col_idx in range(1, min(len(row), len(data["headers"]))):
                header = data["headers"][col_idx]
                if header is None:
                    continue
                price = parse_numeric(row[col_idx])
                if price is not None and price >= 0:
                    data["prices"][header] = float(price)

        # Options Outstanding
        elif any(alias in row_label for alias in OPTS_OUT_ALIASES) and "outstanding" in row_label:
            for cell_value in row[1:]:
                val = parse_numeric(cell_value)
                if val is not None and val > 0:
                    data["options_outstanding"] = float(val)
                    break

        # Option Pool Available / Unallocated
        elif (any(alias in row_label for alias in POOL_AVAIL_ALIASES) and ("plan" in row_label or "issuance" in row_label)) \
             or "available for issuance under the plan" in row_label:
            for cell_value in row[1:]:
                val = parse_numeric(cell_value)
                if val is not None and val > 0:
                    data["option_pool_available"] = float(val)
                    break

    return data


def extract_option_ledger(sheet) -> List[Dict[str, Any]]:
    if sheet is None:
        return []
    options: List[Dict[str, Any]] = []
    header_row_idx = None
    col_map: Dict[str, int] = {}

    synonyms = {
        "exercise_price": ["exercise price", "strike", "strike price", "price"],
        "options_outstanding": ["outstanding", "balance", "granted - outstanding", "options"],
        "id": ["id", "stakeholder id", "emp id"],
        "name": ["name", "optionholder", "holder", "employee"],
    }

    def best_key(col_name: str) -> Optional[str]:
        cl = col_name.lower().strip()
        best = None
        for k, alts in synonyms.items():
            if any(a in cl for a in alts):
                best = k
        return best

    for idx, row in enumerate(sheet.iter_rows(min_row=1, max_row=20, values_only=True)):
        tokens = " ".join([str(c or "").lower() for c in row])
        if any(w in tokens for w in ["exercise", "strike", "price"]) and any(w in tokens for w in ["option", "outstanding", "balance"]):
            header_row_idx = idx + 1
            for col_idx, cell in enumerate(row):
                if not cell:
                    continue
                key = best_key(str(cell))
                if key and key not in col_map:
                    col_map[key] = col_idx
            break

    if not header_row_idx or "options_outstanding" not in col_map or "exercise_price" not in col_map:
        return []

    for row in sheet.iter_rows(min_row=header_row_idx + 1, values_only=True):
        try:
            outstanding = row[col_map["options_outstanding"]] if col_map["options_outstanding"] < len(row) else None
            exercise_price = row[col_map["exercise_price"]] if col_map["exercise_price"] < len(row) else None
            out_val = parse_numeric(outstanding)
            px_val = parse_numeric(exercise_price)
            if out_val is None or px_val is None or out_val <= 0:
                continue
            entry: Dict[str, Any] = {
                "options_outstanding": float(out_val),
                "exercise_price": float(px_val),
            }
            if "id" in col_map and col_map["id"] < len(row):
                entry["id"] = row[col_map["id"]]
            if "name" in col_map and col_map["name"] < len(row):
                entry["name"] = row[col_map["name"]]
            options.append(entry)
        except Exception:
            continue

    return options


def parse_excel_cap_table(file_bytes: bytes) -> Dict[str, Any]:
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
        cap_table_sheet, option_ledger_sheet = pick_candidate_sheets(workbook)
        cap_table_data = extract_cap_table_structure(cap_table_sheet)
        option_data = extract_option_ledger(option_ledger_sheet)
        return {"cap_table": cap_table_data, "option_ledger": option_data, "source": "excel"}
    except Exception as e:
        raise Exception(f"Failed to parse Excel file: {str(e)}")


# =============================================================================
# LLM prompt & normalization
# =============================================================================

EXTRACTION_SYSTEM_PROMPT = """You are an expert financial analyst specializing in venture capital cap tables.
You receive STRUCTURED data extracted from Excel cap tables.

INPUT FORMAT EXAMPLE:
{
  "cap_table": {
    "totals": {"Common": 8054469, "Series Seed Preferred": 2285713},
    "prices": {"Series Seed Preferred": 0.44, "Series A Preferred Stock": 42.57},
    "options_outstanding": 899337,
    "option_pool_available": 1167233
  },
  "option_ledger": [
    {"options_outstanding": 114286, "exercise_price": 0.81}
  ]
}

INSTRUCTIONS:
1. Create a security entry for every key in cap_table.totals:
   - name = the key
   - shares_outstanding = totals[name]
   - original_investment_per_share = prices[name] or 0.0 if missing
   - liquidation_preference_multiple:
       - preferred: 1.0
       - common/options/other: 0.0
   - seniority:
       - preferred with 1.0x = 1
       - common/options/other = null
   - is_participating = false
   - participation_cap_multiple = 0.0
   - cumulative_dividend_rate = 0.0
   - years_since_issuance = 0.0

2. Group option_ledger by exercise_price; sum options_outstanding for each price.
   For each group, add a security named "Options at $X.XX Exercise Price" with:
     shares_outstanding = sum for that price
     original_investment_per_share = 0.0
     liquidation_preference_multiple = 0.0
     seniority = null
     is_participating = false
     participation_cap_multiple = 0.0
     cumulative_dividend_rate = 0.0
     years_since_issuance = 0.0

3. total_option_pool_shares = cap_table.option_pool_available or 0.

OUTPUT REQUIREMENTS:
- Return ONLY a JSON object conforming to:
  {
    "securities": [
      {
        "name": "string",
        "shares_outstanding": 0.0,
        "original_investment_per_share": 0.0,
        "liquidation_preference_multiple": 0.0,
        "seniority": 1,
        "is_participating": false,
        "participation_cap_multiple": 0.0,
        "cumulative_dividend_rate": 0.0,
        "years_since_issuance": 0.0
      }
    ],
    "total_option_pool_shares": 0.0
  }
- No markdown fences.
- No comments.
"""


def _remap_common_llm_mistakes(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tolerantly repair common model mistakes before Pydantic validation.
    - 'security' -> 'name'
    """
    if not isinstance(obj, dict):
        return obj
    if "securities" in obj and isinstance(obj["securities"], list):
        fixed_list = []
        for item in obj["securities"]:
            if isinstance(item, dict):
                if "name" not in item and "security" in item:
                    item["name"] = item.pop("security")
            fixed_list.append(item)
        obj["securities"] = fixed_list
    return obj


async def call_llm(document_text: str) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="Server misconfiguration: OpenAI API key missing")

    if OPENAI_IS_V1:
        def _do_call():
            return client.chat.completions.create(
                model="gpt-4o-mini",
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=1500,
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Extract capital structure:\n\n{document_text}"},
                ],
                timeout=30_000,  # ms
            )
        resp = await run_in_threadpool(_do_call)
        content = resp.choices[0].message.content
    else:
        def _do_call_legacy():
            return openai.ChatCompletion.create(  # type: ignore
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Extract capital structure:\n\n{document_text}"},
                ],
                temperature=0.1,
                max_tokens=1500,
            )
        resp = await run_in_threadpool(_do_call_legacy)
        content = resp.choices[0].message.content.strip()  # type: ignore
        if content.startswith("```"):
            content = content.split("```", 2)[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()

    try:
        parsed = json.loads(content)
    except Exception:
        logger.error("Failed to parse LLM JSON; first 400 chars: %s", content[:400])
        raise HTTPException(status_code=502, detail="Failed to parse AI response")

    repaired = _remap_common_llm_mistakes(parsed)
    return repaired


# =============================================================================
# Routes
# =============================================================================

@app.get("/")
async def root():
    return {"message": "Capital Structure API", "version": "2.2.0"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/debug/cors")
async def debug_cors():
    # Useful while configuring; remove once stable.
    return {
        "CORS_ALLOW_CREDENTIALS": os.getenv("CORS_ALLOW_CREDENTIALS"),
        "CORS_ALLOWED_ORIGINS": os.getenv("CORS_ALLOWED_ORIGINS"),
        "CORS_ALLOWED_ORIGIN_REGEX": os.getenv("CORS_ALLOWED_ORIGIN_REGEX"),
        "effective_allow_credentials": ALLOW_CREDENTIALS,
        "effective_allow_origins": allowed_origins,
        "effective_allow_origin_regex": ALLOWED_ORIGIN_REGEX,
    }


@app.post("/api/documents/upload", response_model=DocumentUploadResponse)
async def upload_document(request: FileUploadRequest):
    if "." not in request.file_name:
        raise HTTPException(status_code=400, detail="File must have an extension")
    ext = request.file_name.rsplit(".", 1)[1].lower()
    if ext not in ALLOWED_EXTS:
        if ext == "xls":
            raise HTTPException(status_code=415, detail="Legacy .xls not supported. Please upload .xlsx")
        raise HTTPException(status_code=415, detail=f"Unsupported file type: .{ext}")

    try:
        file_bytes = base64.b64decode(request.file_content, validate=True)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 payload")

    if len(file_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large")

    file_id = f"upload_{uuid.uuid4()}.{ext}"

    def _write_tmp():
        fd, path = tempfile.mkstemp(prefix="cap_struct_", suffix=f"_{file_id}", dir="/tmp")
        with os.fdopen(fd, "wb") as f:
            f.write(file_bytes)
        return path

    try:
        path = await run_in_threadpool(_write_tmp)
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
    except Exception:
        logger.exception("Upload failed")
        raise HTTPException(status_code=400, detail="Upload failed")


@app.post("/api/documents/extract", response_model=CapitalStructureInput)
async def extract_data(payload: DocumentExtractRequest, req: Request):
    rid = str(uuid.uuid4())[:8]
    logger.info("[rid=%s] Extract requested for file_id=%s", rid, payload.file_id)

    if payload.file_id not in file_storage:
        raise HTTPException(status_code=404, detail="File not found")

    meta = file_storage[payload.file_id]
    path = meta["path"]
    ext = meta.get("extension", "txt").lower()
    original_name = meta.get("original_name", "uploaded_file")

    try:
        if ext == "xlsx":
            logger.info("[rid=%s] Parsing Excel file: %s", rid, original_name)

            def _read_bytes():
                with open(path, "rb") as f:
                    return f.read()

            file_bytes = await run_in_threadpool(_read_bytes)

            def _parse():
                return parse_excel_cap_table(file_bytes)

            structured_data = await run_in_threadpool(_parse)

            # Minimal debug without leaking values
            cap_table = structured_data.get("cap_table", {})
            logger.debug("[rid=%s] headers=%s", rid, cap_table.get("headers"))
            logger.debug("[rid=%s] totals_keys=%s", rid, list(cap_table.get("totals", {}).keys()))
            document_text = json.dumps(structured_data, separators=(",", ":"))  # compact
        else:
            logger.info("[rid=%s] Processing as text file: %s", rid, original_name)

            def _read_text():
                with open(path, "rb") as f:
                    b = f.read()
                return b.decode("utf-8", errors="replace")

            document_text = await run_in_threadpool(_read_text)

        # Call LLM and validate
        llm_obj = await call_llm(document_text)
        try:
            result = CapitalStructureInput.model_validate(llm_obj)
            return result
        except Exception as e:
            # Log shape only
            shapes = []
            try:
                for s in (llm_obj.get("securities", []) or [])[:3]:
                    if isinstance(s, dict):
                        shapes.append(sorted(list(s.keys())))
            except Exception:
                pass
            logger.error("[rid=%s] Validation error. securities[0..2] keys: %s; error=%s", rid, shapes, str(e))
            raise HTTPException(status_code=502, detail="AI response failed validation")

    except HTTPException:
        raise
    except Exception:
        logger.exception("[rid=%s] Unexpected error in extract_data", rid)
        raise HTTPException(status_code=500, detail="Extraction failed")


# =============================================================================
# Entrypoint
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
