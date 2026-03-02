from __future__ import annotations
from pathlib import Path
from typing import List, Optional, Tuple
import os
import time
import hmac
import uuid
from collections import deque

from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Header, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
import pandas as pd
import asyncio

from trackb_engine.config import (
    DEFAULT_EMISSION_FACTOR,
    FEATURE_STORE_DIR,
    FEATURE_STORE_UPLOADS_DIR,
    MAX_UPLOAD_BYTES,
    ALLOWED_UPLOAD_EXTS,
    UPLOAD_RATE_LIMIT,
    UPLOAD_RATE_WINDOW_SEC,
    BACKOFF_MAX_SECONDS,
)
from trackb_engine.feature_store import load_or_build_pipeline
from trackb_engine.golden import GoldenSignatureManager
from trackb_engine.realtime import generate_adaptive_recommendations, compare_batch_to_signature, estimate_roi
from trackb_engine.telemetry import log_event


app = FastAPI(title="AGPO FastAPI Backend", version="1.0")

# ── SECURITY FIX 1: Wildcard CORS with allow_credentials=True is prohibited by
# browsers (CORS spec §3.2.2).  Only allow explicitly configured origins.
# Set AGPO_CORS_ORIGINS env var to a comma-separated list of trusted origins.
# Default falls back to no credentials for wildcard to avoid silent breakage.
_raw_origins = os.environ.get("AGPO_CORS_ORIGINS", "")
_allow_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()] if _raw_origins else []
_allow_credentials = bool(_allow_origins)  # Only allow credentials when origins are explicit

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins if _allow_origins else ["*"],
    allow_credentials=_allow_credentials,
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key", "Content-Type", "Authorization"],
    max_age=3600,
)


# ── SECURITY FIX 2: Add security response headers (CSP, X-Frame-Options, etc.) ─
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        resp: Response = await call_next(request)
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["X-XSS-Protection"] = "1; mode=block"
        resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        resp.headers["Content-Security-Policy"] = (
            "default-src 'none'; frame-ancestors 'none'"
        )
        resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return resp

app.add_middleware(SecurityHeadersMiddleware)


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
    else:
        return


class RateLimiter:
    def __init__(self) -> None:
        self._buckets: dict[str, deque] = {}

    def allow(self, key: str, max_count: int, window_seconds: int) -> Tuple[bool, int]:
        now = time.time()
        bucket = self._buckets.setdefault(key, deque())
        while bucket and now - bucket[0] > window_seconds:
            bucket.popleft()
        if len(bucket) >= max_count:
            backoff = min(int(window_seconds - (now - bucket[0])) + 1, int(BACKOFF_MAX_SECONDS))
            return False, backoff
        bucket.append(now)
        return True, 0


_limiter = RateLimiter()


@app.get("/health")
def health(dep: None = Depends(require_api_key)) -> dict:
    return {"status": "ok"}


@app.post("/upload")
async def upload_files(
    request: Request,
    production: UploadFile = File(...),
    process: UploadFile = File(...),
    # SECURITY FIX 3: Use Depends() so require_api_key runs via FastAPI DI;
    # the old `await require_api_key()` call never passed the header value,
    # meaning ANY caller bypassed authentication on this endpoint.
    dep: None = Depends(require_api_key),
) -> dict:
    # SECURITY FIX 4: Narrow the exception so rate-limit enforcement cannot be
    # silently swallowed.  Only catch ValueError from the limiter calc itself.
    client_ip = request.client.host if request.client else "unknown"
    client_key = f"upload:{client_ip}"
    ok, wait = _limiter.allow(client_key, int(UPLOAD_RATE_LIMIT), int(UPLOAD_RATE_WINDOW_SEC))
    if not ok:
        log_event("api_upload_rate_limited", {"wait_seconds": wait, "client": client_ip})
        raise HTTPException(status_code=429, detail=f"Rate limited. Retry in {wait}s")
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
                    if suffix == ".xlsx":
                        if not chunk.startswith(b"PK"):
                            fh.close()
                            dest.unlink(missing_ok=True)
                            raise HTTPException(status_code=400, detail="Invalid XLSX file")
                    else:
                        try:
                            _ = chunk.decode("utf-8", errors="strict")
                        except Exception:
                            fh.close()
                            dest.unlink(missing_ok=True)
                            raise HTTPException(status_code=400, detail="CSV must be UTF-8 text")
                    first_chunk = False
                fh.write(chunk)
        path = str(dest.resolve())
        log_event("api_upload_saved", {"path": path, "bytes": size})
        return path

    prod_path = await _save(production)
    proc_path = await _save(process)
    return {"production_file": prod_path, "process_file": proc_path}


_PIPELINE_CACHE: dict = {}

def _cache_key(prod: str, proc: str, ef: float, use_store: bool, force_rebuild: bool) -> Tuple[str, str, float, bool, bool, float, float]:
    p1 = Path(prod)
    p2 = Path(proc)
    m1 = p1.stat().st_mtime if p1.exists() else 0.0
    m2 = p2.stat().st_mtime if p2.exists() else 0.0
    return (str(p1), str(p2), float(ef), bool(use_store), bool(force_rebuild), float(m1), float(m2))

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

@app.post("/pipeline/run")
async def pipeline_run(req: PipelineRequest, dep: None = Depends(require_api_key)) -> dict:
    ok, wait = _limiter.allow("pipeline", 60, 60)
    if not ok:
        log_event("api_pipeline_rate_limited", {"wait_seconds": wait})
        raise HTTPException(status_code=429, detail=f"Rate limited. Retry in {wait}s")
    prod = _safe_path(req.production_file)
    proc = _safe_path(req.process_file)
    key = _cache_key(prod, proc, req.emission_factor, req.use_feature_store, req.force_rebuild)
    cached = _PIPELINE_CACHE.get(key)
    if cached and time.time() - cached.get("ts", 0) < 300:
        return {**cached["data"], "cache": "hit"}
    artifacts, info = await asyncio.to_thread(
        load_or_build_pipeline,
        prod,
        proc,
        req.emission_factor,
        FEATURE_STORE_DIR,
        req.use_feature_store,
        req.force_rebuild,
    )
    features = artifacts.features
    resp = {
        "features_count": int(len(features)),
        "batch_ids": features["Batch_ID"].astype(str).tolist(),
        "cleaning_report": artifacts.cleaning_report,
        "cache_info": info,
    }
    _PIPELINE_CACHE[key] = {"ts": time.time(), "data": resp}
    if len(_PIPELINE_CACHE) > 2:
        _PIPELINE_CACHE.pop(next(iter(_PIPELINE_CACHE)))
    log_event("api_pipeline_run", {"features": int(len(features))})
    return resp


@app.get("/golden")
def golden_signature(dep: None = Depends(require_api_key)) -> dict:
    # SECURITY FIX 5: GoldenSignatureManager requires storage_path — calling
    # it with no arguments caused an immediate TypeError (runtime crash).
    from trackb_engine.config import GOLDEN_SIGNATURE_FILE
    manager = GoldenSignatureManager(storage_path=GOLDEN_SIGNATURE_FILE)
    payload = manager.load() or {}
    return payload


@app.post("/batch/recommendations")
async def batch_recommendations(req: BatchRecommendationRequest, dep: None = Depends(require_api_key)) -> dict:
    ok, wait = _limiter.allow("recommendations", 120, 60)
    if not ok:
        log_event("api_recommendations_rate_limited", {"wait_seconds": wait})
        raise HTTPException(status_code=429, detail=f"Rate limited. Retry in {wait}s")
    prod = _safe_path(req.production_file)
    proc = _safe_path(req.process_file)
    artifacts, _ = await asyncio.to_thread(
        load_or_build_pipeline,
        prod,
        proc,
        req.emission_factor,
        FEATURE_STORE_DIR,
        req.use_feature_store,
        req.force_rebuild,
    )
    f = artifacts.features
    if "Batch_ID" not in f.columns:
        raise HTTPException(status_code=400, detail="Batch_ID column missing in features")
    rows = f.loc[f["Batch_ID"].astype(str) == str(req.batch_id)]
    if rows.empty:
        raise HTTPException(status_code=404, detail="Batch not found")
    current = rows.iloc[0].to_dict()

    manager = GoldenSignatureManager()
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
