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
from collections import defaultdict

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
    version="9.0.0-deterministic",
    description="API using a deterministic-first approach for fast and reliable extraction."
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "https://capital-structure-frontend.vercel.app"],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

file_storage: Dict[str, Dict[str, Any]] = {}

# Pydantic models remain the same
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
# 3. Deterministic-First Parsing & Anonymization Logic
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

def normalize_header(header: str) -> str:
    return re.sub(r'\s+', ' ', str(header or '')).strip().lower()

async def get_column_mapping_from_llm(headers: List[str], sample_rows: List[List[Any]], target_fields: List[str]) -> Dict[str, str]:
    if not client: raise ValueError("LLM client not configured, cannot map columns.")
    logger.info(f"Using LLM to map columns for headers: {headers}")
    try:
        def _do_call():
            return client.chat.completions.create(
                model="gpt-4o-mini",
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": "Map the provided noisy headers to the target schema fields. Reply with a JSON object where keys are target fields and values are the best matching original headers. If a field cannot be matched, omit it from the JSON."},
                    {"role": "user", "content": json.dumps({
                        "target_fields": target_fields,
                        "headers": headers,
                        "sample_rows": sample_rows
                    })}
                ],
                temperature=0, timeout=20.0
            )
        resp = await run_in_threadpool(_do_call)
        mapping = json.loads(resp.choices[0].message.content or '{}')
        return {v: k for k, v in mapping.items()} # Invert to map: header -> target_field
    except Exception as e:
        logger.error(f"LLM column mapping failed: {e}")
        return {} # Fallback to empty map on failure

def parse_sheet_deterministically(sheet, header_aliases: Dict[str, str], anonymizer: Anonymizer, anonymize_column: Optional[str] = None) -> List[Dict[str, Any]]:
    rows = list(sheet.iter_rows(values_only=True))
    if not rows: return []
    
    header_row_index = -1
    for i, row in enumerate(rows[:20]): # Search for header in first 20 rows
        normalized_row = [normalize_header(cell) for cell in row]
        if len([h for h in normalized_row if h in header_aliases]) > 1:
            header_row_index = i
            break
    
    if header_row_index == -1: return [] # Could not find a valid header row

    headers = [normalize_header(cell) for cell in rows[header_row_index]]
    header_map = {h: header_aliases.get(h) for h in headers if h in header_aliases}
    
    anonymize_col_idx = -1
    if anonymize_column:
        try: anonymize_col_idx = headers.index(anonymize_column)
        except ValueError: pass

    data_rows = []
    for row in rows[header_row_index + 1:]:
        if not any(row): continue # Skip empty rows
        
        row_data = {}
        for i, cell in enumerate(row):
            header = headers[i]
            if header in header_map:
                target_field = header_map[header]
                # Column-aware anonymization
                if i == anonymize_col_idx:
                    row_data[target_field] = anonymizer.anonymize_name(str(cell))
                else:
                    row_data[target_field] = cell
        if row_data:
            data_rows.append(row_data)
            
    return data_rows

# =============================================================================
# 4. Main Extraction Orchestration
# =============================================================================
async def run_deterministic_extraction(file_bytes: bytes, rid: str) -> Dict[str, Any]:
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    except Exception as e:
        raise ValueError(f"Could not load the Excel file. It may be corrupted. Error: {e}")

    anonymizer = Anonymizer()
    
    # Identify sheets
    cap_sheet, option_sheet = None, None
    for sheet in workbook.worksheets:
        title = sheet.title.lower()
        if not cap_sheet and "detailed cap" in title: cap_sheet = sheet
        if not option_sheet and ("option" in title or "grant" in title): option_sheet = sheet
    if not cap_sheet: cap_sheet = workbook.worksheets[0]
    
    # Define header aliases for deterministic parsing
    cap_header_aliases = {
        'class': 'name', 'class of stock': 'name', 'security': 'name',
        'shares outstanding': 'shares', 'total outstanding': 'shares', 'outstanding': 'shares',
        'price per share': 'price', 'original price': 'price',
    }
    option_header_aliases = {
        'optionholder': 'holder', 'name': 'holder',
        'options granted': 'shares', 'shares': 'shares', 'amount': 'shares',
        'exercise price': 'price', 'strike price': 'price',
    }

    # Deterministically parse sheets
    cap_data = parse_sheet_deterministically(cap_sheet, cap_header_aliases, anonymizer, anonymize_column='stakeholder') if cap_sheet else []
    option_data = parse_sheet_deterministically(option_sheet, option_header_aliases, anonymizer, anonymize_column='optionholder') if option_sheet else []
    
    # Aggregate results in Python
    securities = []
    seen_securities = set()

    for item in cap_data:
        name = item.get('name')
        if not name or not isinstance(name, str) or name.lower() in seen_securities: continue
        
        shares = float(str(item.get('shares', 0)).replace(',',''))
        if shares <= 0: continue

        securities.append({
            "name": name.strip(),
            "shares_outstanding": shares,
            "original_investment_per_share": float(str(item.get('price', 0)).replace(',','')),
            # Set defaults, as this info is often not in the main table
            "liquidation_preference_multiple": 1.0 if "preferred" in name.lower() else 0.0,
            "seniority": 1 if "preferred" in name.lower() else None,
            "is_participating": False, "participation_cap_multiple": 0.0,
            "cumulative_dividend_rate": 0.0, "years_since_issuance": 0.0
        })
        seen_securities.add(name.lower())

    # Group options by exercise price
    options_by_price = defaultdict(float)
    for item in option_data:
        try:
            price = float(str(item.get('price', 0)).replace('$', '').replace(',', ''))
            shares = float(str(item.get('shares', 0)).replace(',', ''))
            if price > 0 and shares > 0:
                options_by_price[price] += shares
        except (ValueError, TypeError):
            continue
            
    for price, total_shares in options_by_price.items():
        securities.append({
            "name": f"Options at ${price:.2f} Exercise Price",
            "shares_outstanding": total_shares,
            "original_investment_per_share": 0.0, "liquidation_preference_multiple": 0.0,
            "seniority": None, "is_participating": False, "participation_cap_multiple": 0.0,
            "cumulative_dividend_rate": 0.0, "years_since_issuance": 0.0
        })

    # This part can be improved by a more robust search for the option pool
    total_option_pool_shares = 0.0
    
    return {"securities": securities, "total_option_pool_shares": total_option_pool_shares}


# =============================================================================
# 5. API Routes
# =============================================================================

@app.get("/", include_in_schema=False)
async def root(): return {"message": "Capital Structure API", "version": app.version}

@app.get("/health", tags=["Health"])
async def health(): return {"status": "healthy"}

@app.post("/api/documents/upload", response_model=DocumentUploadResponse, status_code=201, tags=["Processing"])
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

@app.post("/api/documents/extract", response_model=CapitalStructureInput, tags=["Processing"])
async def extract_data(payload: DocumentExtractRequest):
    rid = str(uuid.uuid4())[:8]
    logger.info(f"[rid={rid}] Extraction requested for file_id={payload.file_id}")
    if payload.file_id not in file_storage: raise HTTPException(status_code=404, detail="File not found.")
    path = file_storage[payload.file_id].get("path")
    if not path or not os.path.exists(path):
        logger.error(f"[rid={rid}] File missing at path: {path}")
        raise HTTPException(status_code=410, detail="File has expired.")
    
    try:
        def _read_and_process():
            with open(path, "rb") as f:
                return f.read()
        
        file_bytes = await run_in_threadpool(_read_and_process)
        
        # Run the new deterministic extraction pipeline
        result_data = await run_deterministic_extraction(file_bytes, rid)
        
        # Validate the final output against our Pydantic model
        result = CapitalStructureInput.model_validate(result_data)
        
        if not result.securities:
            raise ValueError("Deterministic parsing resulted in an empty list of securities.")
        return result

    except (ValueError, ValidationError) as e:
        # These are now expected business logic failures (e.g., bad format)
        logger.warning(f"[rid={rid}] Data validation or parsing failed: {e}")
        raise HTTPException(status_code=400, detail=f"Could not process document: {e}")
    except HTTPException:
        raise
    except Exception:
        logger.exception(f"[rid={rid}] An unexpected, unhandled error occurred during extraction.")
        raise HTTPException(status_code=500, detail="An unexpected server error occurred.")
        
    finally:
        try:
            if path and os.path.exists(path):
                os.remove(path); logger.info(f"[rid={rid}] Cleaned up temp file: {path}")
            if payload.file_id in file_storage:
                del file_storage[payload.file_id]
        except Exception as e:
            logger.error(f"[rid={rid}] CRITICAL: Failed to clean up temp file {path}: {e}")

# =============================================================================
# 6. Server Entrypoint
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting server on port {PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
