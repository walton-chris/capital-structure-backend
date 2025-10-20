import os, base64, uuid, json, tempfile, binascii, hashlib, logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [web] - %(message)s"
)
logger = logging.getLogger("web")

PORT = int(os.getenv("PORT", "8080"))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", "25000000"))
ALLOWED_EXTS = {"xlsx"}

app = FastAPI(
    title="Capital Structure API",
    version="10.0.1-jobs",
    description="Asynchronous deterministic-first extraction with optional LLM assist."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "https://capital-structure-frontend.vercel.app",
    ],
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    max_age=600,
)

_redis = None
_q = None

def get_redis():
    """Lazy init so the app boots even if Redis is briefly unavailable."""
    global _redis
    if _redis is not None:
        return _redis
    from redis import Redis
    url = os.getenv("REDIS_URL")
    if not url:
        raise RuntimeError("REDIS_URL is not set")
    _redis = Redis.from_url(url)
    return _redis

def get_queue():
    global _q
    if _q is not None:
        return _q
    from rq import Queue
    _q = Queue("extract", connection=get_redis())
    return _q

class FileUploadRequest(BaseModel):
    file_content: str
    file_name: str

class DocumentUploadResponse(BaseModel):
    file_id: str
    file_name: str
    file_sha256: str
    message: str
    file_size_bytes: int

class DocumentExtractRequest(BaseModel):
    file_id: str
    file_sha256: str
    callback_url: Optional[str] = None

FILES: Dict[str, Dict[str, Any]] = {}

@app.options("/{full_path:path}")
async def preflight(full_path: str, request: Request):
    return Response(status_code=204)

@app.get("/", include_in_schema=False)
async def root():
    return {"message": "Capital Structure API", "version": app.version}

@app.get("/health")
async def health():
    status = {"status": "healthy", "redis": "ok"}
    try:
        r = get_redis()
        r.ping()
    except Exception as e:
        status["status"] = "degraded"
        status["redis"] = f"error: {e.__class__.__name__}: {e}"
    return status

@app.post("/api/documents/upload", response_model=DocumentUploadResponse, status_code=201)
async def upload_document(req: FileUploadRequest):
    try:
        file_bytes = base64.b64decode(req.file_content, validate=True)
    except (ValueError, binascii.Error):
        raise HTTPException(status_code=400, detail="Invalid base64 content.")
    if len(file_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File size exceeds limit.")

    ext = req.file_name.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_EXTS:
        raise HTTPException(status_code=415, detail="Please upload an .xlsx file.")

    file_id = f"upload_{uuid.uuid4()}.{ext}"
    sha256 = hashlib.sha256(file_bytes).hexdigest()

    try:
        with tempfile.NamedTemporaryFile(delete=False, prefix="cap_struct_", suffix=f"_{file_id}", dir="/tmp") as tmp:
            tmp.write(file_bytes)
            path = tmp.name
        FILES[file_id] = {"path": path, "original_name": req.file_name, "sha256": sha256}
        return DocumentUploadResponse(
            file_id=file_id,
            file_name=req.file_name,
            file_sha256=sha256,
            message="File uploaded successfully",
            file_size_bytes=len(file_bytes),
        )
    except Exception:
        logger.exception("Failed to write temp file")
        raise HTTPException(status_code=500, detail="Failed to save file on server.")

@app.post("/api/documents/extract")
async def submit_extract(payload: DocumentExtractRequest):
    try:
        r = get_redis()
        q = get_queue()
    except Exception as e:
        logger.error(f"Background infra unavailable: {e}")
        raise HTTPException(status_code=503, detail="Background processing unavailable. Please retry later.")

    cache_key = f"result:{payload.file_sha256}"
    cached = r.get(cache_key)
    if cached:
        logger.info(f"Cache hit for sha256: {payload.file_sha256}")
        return {"job_id": None, "status": "succeeded", "result": json.loads(cached)}

    fi = FILES.get(payload.file_id)
    if not fi:
        raise HTTPException(status_code=404, detail="File not found. Please upload again.")
    if not os.path.exists(fi.get("path", "")):
        raise HTTPException(status_code=410, detail="File has expired. Please upload again.")
    if fi.get("sha256") != payload.file_sha256:
        raise HTTPException(status_code=400, detail="File content mismatch (SHA256). Please re-upload.")

    job = q.enqueue(
        "worker.run_extract_job",
        file_path=fi["path"],
        file_sha256=payload.file_sha256,
        callback_url=payload.callback_url,
        job_timeout=600,
        result_ttl=86400,
        failure_ttl=86400,
    )
    logger.info(f"Enqueued job {job.id} for file_id {payload.file_id}")
    return {"job_id": job.id, "status": "queued"}

@app.get("/api/jobs/{job_id}")
async def get_job_status(job_id: str):
    try:
        from rq.job import Job
        job = Job.fetch(job_id, connection=get_redis())
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found.")
    status = job.get_status()
    if job.is_finished:
        return {"status": "succeeded", "result": job.return_value()}
    if job.is_failed:
        return {"status": "failed", "error": "Processing failed in the background worker."}
    return {"status": status}
