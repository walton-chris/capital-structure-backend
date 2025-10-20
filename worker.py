import os, io, json, uuid, logging, re
from typing import Any, Dict, List, Optional
from collections import defaultdict
import openpyxl
from pydantic import BaseModel, Field, ValidationError, NonNegativeFloat, field_validator
from redis import Redis
import requests

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [worker] - %(message)s"
)
logger = logging.getLogger("worker")

# ---- Lazy Redis (avoid crashing on import) ----
_redis = None
def get_redis() -> Redis:
    global _redis
    if _redis is not None:
        return _redis
    url = os.getenv("REDIS_URL")
    if not url:
        raise RuntimeError("REDIS_URL is not set")
    _redis = Redis.from_url(url)
    return _redis

# ---- Pydantic models ----
class Security(BaseModel):
    name: str
    shares_outstanding: NonNegativeFloat
    original_investment_per_share: NonNegativeFloat
    liquidation_preference_multiple: NonNegativeFloat
    seniority: Optional[int] = Field(default=None, ge=0, le=10)
    is_participating: bool
    participation_cap_multiple: NonNegativeFloat
    cumulative_dividend_rate: NonNegativeFloat
    years_since_issuance: NonNegativeFloat

    @field_validator("name")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Security 'name' cannot be empty.")
        return v.strip()

class CapitalStructureInput(BaseModel):
    securities: List[Security]
    total_option_pool_shares: NonNegativeFloat

TOTALS_RE = re.compile(r"\b(total|subtotal|grand\s+total)\b", re.IGNORECASE)

class Anonymizer:
    def __init__(self):
        self.map: Dict[str,str] = {}
        self.person_counter = 1
        self.entity_counter = 1
        self.entity_re = re.compile(r"(LLC|Inc|LP|FBO|Capital|Partners|Fund|Trust|Ventures)", re.IGNORECASE)
    def maybe(self, name: Any) -> Any:
        if not isinstance(name, str):
            return name
        key = name.strip()
        if not key:
            return name
        if key in self.map:
            return self.map[key]
        is_entity = bool(self.entity_re.search(key)) or key.isupper()
        ph = f"Entity-{self.entity_counter}" if is_entity else f"Person-{self.person_counter}"
        if is_entity: self.entity_counter += 1
        else: self.person_counter += 1
        self.map[key] = ph
        return ph

def normalize_header(h: Any) -> str:
    return re.sub(r"\s+", " ", str(h or "")).strip().lower()

def as_float(x: Any, default: float=0.0) -> float:
    try:
        return float(str(x).replace(",","").replace("$",""))
    except (ValueError, TypeError):
        return default

def parse_sheet(sheet, header_aliases, anonymizer, anonymize_headers):
    rows = list(sheet.iter_rows(values_only=True))
    if not rows:
        return []
    header_idx, best_hits = -1, -1
    for i, row in enumerate(rows[:20]):
        norm = [normalize_header(c) for c in row]
        hits = sum(1 for h in norm if h in header_aliases)
        if hits > best_hits and hits >= 2:
            header_idx, best_hits = i, hits
    if header_idx < 0:
        return []
    headers = [normalize_header(c) for c in rows[header_idx]]
    header_map = {h: header_aliases[h] for h in headers if h in header_aliases}
    anon_idx = {i for i, h in enumerate(headers) if h in anonymize_headers}
    data: List[Dict[str,Any]] = []
    for row in rows[header_idx+1:]:
        if not any(row) or any(isinstance(c, str) and TOTALS_RE.search(c) for c in row if c):
            continue
        rec: Dict[str,Any] = {}
        for i, cell in enumerate(row):
            if i >= len(headers):
                break
            src_h = headers[i]
            if src_h not in header_map:
                continue
            dst = header_map[src_h]
            val = anonymizer.maybe(cell) if i in anon_idx else cell
            rec[dst] = val
        if rec:
            data.append(rec)
    return data

def run_extract_job(file_path: str, file_sha256: str, callback_url: Optional[str] = None) -> Dict[str, Any]:
    rid = str(uuid.uuid4())[:8]
    logger.info(f"[rid={rid}] Starting job for file: {file_path}")
    r = get_redis()
    cache_key = f"result:{file_sha256}"

    with open(file_path, "rb") as f:
        file_bytes = f.read()
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)

    anonymizer = Anonymizer()
    cap_sheet, opt_sheet = None, None
    for sh in wb.worksheets:
        t = (sh.title or "").lower()
        if not cap_sheet and ("detailed cap" in t or "cap table" in t):
            cap_sheet = sh
        if not opt_sheet and ("option" in t or "grant" in t):
            opt_sheet = sh
    if not cap_sheet:
        cap_sheet = wb.worksheets[0]

    cap_aliases = {
        "class":"name", "class of stock":"name", "security":"name",
        "shares outstanding":"shares", "total outstanding":"shares", "outstanding":"shares",
        "price per share":"price", "original price":"price",
    }
    opt_aliases = {
        "optionholder":"holder", "holder":"holder", "name":"holder",
        "options granted":"shares", "shares":"shares", "amount":"shares",
        "exercise price":"price", "strike price":"price",
    }

    cap_rows = parse_sheet(cap_sheet, cap_aliases, anonymizer, anonymize_headers=("stakeholder",))
    opt_rows = parse_sheet(opt_sheet, opt_aliases, anonymizer, anonymize_headers=("optionholder","holder","name")) if opt_sheet else []

    securities: List[Dict[str, Any]] = []
    seen = set()
    for rrow in cap_rows:
        name = (rrow.get("name") or "").strip()
        if not name or name.lower() in seen:
            continue
        shares = as_float(rrow.get("shares"), 0.0)
        if shares <= 0:
            continue
        price = as_float(rrow.get("price"), 0.0)
        is_pref = "preferred" in name.lower()
        securities.append({
            "name": name,
            "shares_outstanding": shares,
            "original_investment_per_share": price,
            "liquidation_preference_multiple": 1.0 if is_pref else 0.0,
            "seniority": 1 if is_pref else None,
            "is_participating": False,
            "participation_cap_multiple": 0.0,
            "cumulative_dividend_rate": 0.0,
            "years_since_issuance": 0.0
        })
        seen.add(name.lower())

    options_by_px = defaultdict(float)
    for rrow in opt_rows:
        px = as_float(rrow.get("price"), 0.0)
        sh = as_float(rrow.get("shares"), 0.0)
        if px > 0 and sh > 0:
            options_by_px[px] += sh
    for px, total in options_by_px.items():
        securities.append({
            "name": f"Options at ${px:.2f} Exercise Price",
            "shares_outstanding": total,
            "original_investment_per_share": 0.0,
            "liquidation_preference_multiple": 0.0,
            "seniority": None,
            "is_participating": False,
            "participation_cap_multiple": 0.0,
            "cumulative_dividend_rate": 0.0,
            "years_since_issuance": 0.0
        })

    result_obj = {"securities": securities, "total_option_pool_shares": 0.0}
    validated = CapitalStructureInput.model_validate(result_obj)
    result = json.loads(validated.model_dump_json())

    r.setex(cache_key, 86400, json.dumps(result))
    if callback_url:
        try:
            requests.post(callback_url, json={"status": "succeeded", "result": result}, timeout=5)
        except Exception as e:
            logger.warning(f"[rid={rid}] Webhook failed: {e}")

    try:
        os.remove(file_path)
        logger.info(f"[rid={rid}] Cleaned up temp file: {file_path}")
    except OSError as e:
        logger.warning(f"[rid={rid}] Could not clean up temp file: {e}")

    logger.info(f"[rid={rid}] Job completed successfully.")
    return result
