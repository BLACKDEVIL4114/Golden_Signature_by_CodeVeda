from __future__ import annotations

import asyncio
import hmac
import logging
import os
import uuid
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware

from trackb_engine.config import (
    ALLOWED_UPLOAD_EXTS,
    DEFAULT_EMISSION_FACTOR,
    FEATURE_STORE_DIR,
    FEATURE_STORE_UPLOADS_DIR,
    GOLDEN_SIGNATURE_FILE,
    MAX_UPLOAD_BYTES,
)
from trackb_engine.feature_store import load_or_build_pipeline
from trackb_engine.golden import GoldenSignatureManager
from trackb_engine.realtime import compare_batch_to_signature, estimate_roi, generate_adaptive_recommendations
from trackb_engine.telemetry import log_event

load_dotenv()
logger = logging.getLogger(__name__)

app = FastAPI(title="AGPO FastAPI Backend", version="1.0")

_raw_origins = os.environ.get("AGPO_CORS_ORIGINS", "")
_allow_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()] if _raw_origins else []
_allow_credentials = bool(_allow_origins)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins if _allow_origins else ["*"],
    allow_credentials=_allow_credentials,
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key", "Content-Type", "Authorization"],
    max_age=3600,
)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        resp: Response = await call_next(request)
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["X-XSS-Protection"] = "1; mode=block"
        resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        resp.headers["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none'"
        resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return resp


app.add_middleware(SecurityHeadersMiddleware)

limiter = Limiter(key_func=get_remote_address, default_limits=["100/minute"])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


class PipelineRequest(BaseModel):
    production_file: str
    process_file: str
    emission_factor: float = DEFAULT_EMISSION_FACTOR
    use_feature_store: bool = True
    force_rebuild: bool = False


class BatchRecommendationRequest(BaseModel):
    production_file: str
    process_file: str
    batch_id: str
    emission_factor: float = DEFAULT_EMISSION_FACTOR
    use_feature_store: bool = True
    force_rebuild: bool = False


def require_api_key(x_api_key: Optional[str] = Header(None)) -> None:
    expected = os.environ.get("AGPO_API_KEY")
    if expected:
        if not x_api_key or not hmac.compare_digest(str(x_api_key), str(expected)):
            raise HTTPException(status_code=401, detail="Invalid API key")


def _safe_path(user_path: str) -> str:
    base1 = Path(FEATURE_STORE_UPLOADS_DIR).resolve()
    base2 = Path.cwd().resolve()
    p = Path(user_path).resolve()
    if p.is_absolute():
        if str(p).startswith(str(base1)) or str(p).startswith(str(base2)):
            return str(p)
        raise HTTPException(status_code=400, detail="Invalid path")
    rp = (base2 / user_path).resolve()
    if str(rp).startswith(str(base2)):
        return str(rp)
    raise HTTPException(status_code=400, detail="Invalid path")


@app.get("/health")
@limiter.limit("100/minute")
def health(request: Request, dep: None = Depends(require_api_key)) -> dict:
    return {"status": "ok"}


@app.post("/upload")
@limiter.limit("100/minute")
async def upload_files(
    request: Request,
    production: UploadFile = File(...),
    process: UploadFile = File(...),
    dep: None = Depends(require_api_key),
) -> dict:
    for uf in [production, process]:
        suffix = Path(uf.filename).suffix.lower()
        if suffix not in {".csv", ".xlsx"} or suffix.replace(".", "") not in ALLOWED_UPLOAD_EXTS:
            raise HTTPException(status_code=400, detail="Only CSV/XLSX files are allowed")

    uploads_dir = Path(FEATURE_STORE_UPLOADS_DIR)
    uploads_dir.mkdir(parents=True, exist_ok=True)

    async def _save(uf: UploadFile) -> str:
        suffix = Path(uf.filename).suffix.lower()
        safe_name = f"{uuid.uuid4().hex}{suffix}"
        dest = uploads_dir / safe_name
        size = 0
        first_chunk = True
        try:
            with dest.open("wb") as fh:
                while True:
                    chunk = await uf.read(8192)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MAX_UPLOAD_BYTES:
                        fh.close()
                        dest.unlink(missing_ok=True)
                        raise HTTPException(status_code=413, detail="File too large")
                    if first_chunk:
                        if suffix == ".xlsx" and not chunk.startswith(b"PK"):
                            fh.close()
                            dest.unlink(missing_ok=True)
                            raise HTTPException(status_code=400, detail="Invalid XLSX file")
                        if suffix == ".csv":
                            try:
                                _ = chunk.decode("utf-8", errors="strict")
                            except Exception:
                                fh.close()
                                dest.unlink(missing_ok=True)
                                raise HTTPException(status_code=400, detail="CSV must be UTF-8 text")
                        first_chunk = False
                    fh.write(chunk)
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Failed while saving upload: %s", uf.filename)
            raise HTTPException(status_code=500, detail="Failed to save uploaded file") from exc
        path = str(dest.resolve())
        log_event("api_upload_saved", {"path": path, "bytes": size})
        return path

    prod_path = await _save(production)
    proc_path = await _save(process)
    return {"production_file": prod_path, "process_file": proc_path}


_PIPELINE_CACHE: dict = {}


def _cache_key(prod: str, proc: str, ef: float, use_store: bool, force_rebuild: bool):
    p1 = Path(prod)
    p2 = Path(proc)
    m1 = p1.stat().st_mtime if p1.exists() else 0.0
    m2 = p2.stat().st_mtime if p2.exists() else 0.0
    return (str(p1), str(p2), float(ef), bool(use_store), bool(force_rebuild), float(m1), float(m2))


@app.post("/pipeline/run")
@limiter.limit("100/minute")
async def pipeline_run(request: Request, req: PipelineRequest, dep: None = Depends(require_api_key)) -> dict:
    prod = _safe_path(req.production_file)
    proc = _safe_path(req.process_file)
    key = _cache_key(prod, proc, req.emission_factor, req.use_feature_store, req.force_rebuild)
    cached = _PIPELINE_CACHE.get(key)
    if cached:
        return {**cached, "cache": "hit"}

    try:
        artifacts, info = await asyncio.to_thread(
            load_or_build_pipeline,
            prod,
            proc,
            req.emission_factor,
            FEATURE_STORE_DIR,
            req.use_feature_store,
            req.force_rebuild,
        )
    except Exception as exc:
        logger.exception("Pipeline run failed for files %s and %s", prod, proc)
        log_event("api_pipeline_failed", {"reason": str(exc)})
        raise HTTPException(status_code=500, detail="Data pipeline failed. Please check your files and try again.") from exc

    features = artifacts.features
    resp = {
        "features_count": int(len(features)),
        "batch_ids": features["Batch_ID"].astype(str).tolist() if "Batch_ID" in features.columns else [],
        "cleaning_report": artifacts.cleaning_report,
        "cache_info": info,
    }
    _PIPELINE_CACHE[key] = resp
    if len(_PIPELINE_CACHE) > 2:
        _PIPELINE_CACHE.pop(next(iter(_PIPELINE_CACHE)))
    log_event("api_pipeline_run", {"features": int(len(features))})
    return resp


@app.get("/golden")
@limiter.limit("100/minute")
def golden_signature(request: Request, dep: None = Depends(require_api_key)) -> dict:
    try:
        manager = GoldenSignatureManager(storage_path=GOLDEN_SIGNATURE_FILE)
        payload = manager.load() or {}
        return payload
    except Exception as exc:
        logger.exception("Failed to load golden signature")
        raise HTTPException(status_code=500, detail="Failed to load golden signature") from exc


@app.post("/batch/recommendations")
@limiter.limit("100/minute")
async def batch_recommendations(request: Request, req: BatchRecommendationRequest, dep: None = Depends(require_api_key)) -> dict:
    prod = _safe_path(req.production_file)
    proc = _safe_path(req.process_file)

    try:
        artifacts, _ = await asyncio.to_thread(
            load_or_build_pipeline,
            prod,
            proc,
            req.emission_factor,
            FEATURE_STORE_DIR,
            req.use_feature_store,
            req.force_rebuild,
        )
    except Exception as exc:
        logger.exception("Failed to build pipeline for recommendations")
        raise HTTPException(status_code=500, detail="Unable to process data for recommendations") from exc

    f = artifacts.features
    if "Batch_ID" not in f.columns:
        raise HTTPException(status_code=400, detail="Batch_ID column missing in features")

    rows = f.loc[f["Batch_ID"].astype(str) == str(req.batch_id)]
    if rows.empty:
        raise HTTPException(status_code=404, detail="Batch not found")
    current = rows.iloc[0].to_dict()

    manager = GoldenSignatureManager(storage_path=GOLDEN_SIGNATURE_FILE)
    payload = manager.load() or {}
    signatures = payload.get("signatures", {})
    selected = signatures.get("Balanced", {})
    golden_profile = selected.get("profile", current)

    recs = generate_adaptive_recommendations(current=current, golden_profile=golden_profile)
    comp = compare_batch_to_signature(current=current, golden_profile=golden_profile)

    cur_energy = float(current.get("Total_Energy_kWh", 0.0))
    gold_energy = float(golden_profile.get("Total_Energy_kWh", cur_energy))
    roi = estimate_roi(
        current_energy_kwh=cur_energy,
        golden_energy_kwh=gold_energy,
        energy_cost_per_kwh=float(8.0),
        annual_batches=int(365),
    )
    log_event("api_batch_recommendations", {"batch_id": str(req.batch_id)})
    return {
        "recommendations": recs,
        "comparison": comp.to_dict(orient="records"),
        "roi": roi,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api:app", host="0.0.0.0", port=8001, reload=True)
