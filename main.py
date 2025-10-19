"""
Capital Structure API - Production Backend
"""
from typing import Optional, Tuple, List
from pydantic import BaseModel, field_validator
from fastapi import FastAPI, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
import uuid
import base64
from pathlib import Path
import traceback
import logging
import os
import openai

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Capital Structure API",
    description="Document upload and AI extraction for 409A valuations",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY
    logger.info("✓ OpenAI API key configured")
else:
    logger.error("✗ OPENAI_API_KEY not found")

MAX_FILE_SIZE_BYTES = 6_000_000
ALLOWED_EXTENSIONS = {".pdf", ".txt", ".docx", ".xlsx"}
file_storage = {}

class Security(BaseModel):
    name: str
    shares_outstanding: float
    original_investment_per_share: float
    liquidation_preference_multiple: float
    seniority: int
    is_participating: bool
    participation_cap_multiple: float
    cumulative_dividend_rate: float
    years_since_issuance: float

class CapitalStructureInput(BaseModel):
    securities: List[Security]
    total_option_pool_shares: float

class FileUploadRequest(BaseModel):
    file_content: str
    file_name: str

    @field_validator("file_name")
    @classmethod
    def validate_filename(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Filename cannot be empty")
        v = v.strip()
        ext = Path(v).suffix.lower()
        if not ext:
            raise ValueError("File must have an extension")
        if ext not in ALLOWED_EXTENSIONS:
            raise ValueError(f"File type not allowed: {ext}")
        if ".." in v or "/" in v or "\\" in v:
            raise ValueError("Invalid characters in filename")
        return v

class DocumentUploadResponse(BaseModel):
    file_id: str
    file_name: str
    message: str
    file_size_bytes: int

class DocumentExtractRequest(BaseModel):
    file_id: str

def sanitize_filename(original_filename: str) -> Tuple[str, str]:
    ext = Path(original_filename).suffix.lower()
    safe_filename = f"{uuid.uuid4()}{ext}"
    file_id = f"upload_{safe_filename}"
    return file_id, safe_filename

def validate_and_decode_base64(content: str) -> bytes:
    if not content or not isinstance(content, str):
        raise ValueError("File content must be a non-empty string")
    if "," in content and "base64" in content[:100].lower():
        content = content.split(",", 1)[1]
    content = content.strip().replace("\n", "").replace("\r", "").replace(" ", "").replace("\t", "")
    if not content:
        raise ValueError("Content empty after cleaning")
    try:
        decoded = base64.b64decode(content, validate=True)
    except Exception as e:
        raise ValueError(f"Invalid base64 encoding: {str(e)}")
    file_size = len(decoded)
    if file_size == 0:
        raise ValueError("File is empty (0 bytes)")
    if file_size > MAX_FILE_SIZE_BYTES:
        size_mb = file_size / 1_000_000
        max_mb = MAX_FILE_SIZE_BYTES / 1_000_000
        raise ValueError(f"File ({size_mb:.2f}MB) exceeds max ({max_mb:.2f}MB)")
    return decoded

@app.get("/")
async def root():
    return {
        "service": "Capital Structure API",
        "version": "2.0.0",
        "status": "healthy"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "openai_configured": bool(OPENAI_API_KEY),
        "max_file_size_mb": MAX_FILE_SIZE_BYTES / 1_000_000,
        "allowed_extensions": sorted(list(ALLOWED_EXTENSIONS))
    }

@app.post("/api/documents/upload", response_model=DocumentUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_document(request: FileUploadRequest):
    try:
        logger.info(f"📤 UPLOAD: {request.file_name}")
        try:
            file_bytes = validate_and_decode_base64(request.file_content)
            logger.info(f"✓ Decoded: {len(file_bytes):,} bytes")
        except ValueError as e:
            logger.error(f"✗ Validation failed: {e}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        
        file_id, safe_filename = sanitize_filename(request.file_name)
        file_storage[file_id] = {
            "content": file_bytes,
            "original_name": request.file_name,
            "size": len(file_bytes)
        }
        
        logger.info(f"✓ Stored: {file_id}")
        return DocumentUploadResponse(
            file_id=file_id,
            file_name=request.file_name,
            message="File uploaded successfully",
            file_size_bytes=len(file_bytes)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Upload error: {type(e).__name__}: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Upload failed")

@app.post("/api/documents/extract", response_model=CapitalStructureInput)
async def extract_data(request: DocumentExtractRequest):
    try:
        logger.info(f"🔍 EXTRACT: {request.file_id}")
        
        if not OPENAI_API_KEY:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="AI service unavailable")
        
        if request.file_id not in file_storage:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")
        
        file_data = file_storage[request.file_id]
        file_bytes = file_data["content"]
        
        try:
            document_text = file_bytes.decode("utf-8")
            logger.info(f"✓ Decoded: {len(document_text):,} chars")
        except UnicodeDecodeError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only text files supported")
        
        if not document_text.strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Document is empty")
        
        system_prompt = """Extract capital structure data and return ONLY valid JSON:
{
  "securities": [{
    "name": "string",
    "shares_outstanding": number,
    "original_investment_per_share": number,
    "liquidation_preference_multiple": number,
    "seniority": number,
    "is_participating": boolean,
    "participation_cap_multiple": number,
    "cumulative_dividend_rate": number,
    "years_since_issuance": number
  }],
  "total_option_pool_shares": number
}"""
        
        try:
            logger.info("🤖 Calling OpenAI...")
            response = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": document_text}
                ],
                temperature=0.1
            )
            response_content = response.choices[0].message.content
            logger.info("✓ AI response received")
        except Exception as e:
            logger.error(f"✗ OpenAI failed: {e}")
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="AI service unavailable")
        
        try:
            result = CapitalStructureInput.model_validate_json(response_content)
            logger.info(f"✓ Parsed {len(result.securities)} securities")
            return result
        except Exception as e:
            logger.error(f"✗ Parse failed: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to parse extracted data")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Extract error: {type(e).__name__}: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Extraction failed")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
