"""Persistent feature-store cache for large-scale deployments."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple
import hmac
import os

import pandas as pd

from .data_pipeline import PipelineArtifacts, run_pipeline
from .telemetry import log_event

CACHE_VERSION = "agpo_feature_store_v1"

def _read_cached_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, low_memory=False)
    for ts_col in ("Timestamp", "timestamp"):
        if ts_col in frame.columns:
            parsed = pd.to_datetime(frame[ts_col], errors="coerce", utc=True)
            if parsed.notna().any():
                frame[ts_col] = parsed
    return frame


def _file_stats(path: str) -> dict:
    p = Path(path)
    stat = p.stat()
    return {
        "path": str(p.resolve()),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def _build_signature(production_file: str, process_file: str, emission_factor: float) -> str:
    payload = {
        "version": CACHE_VERSION,
        "production": _file_stats(production_file),
        "process": _file_stats(process_file),
        "emission_factor": float(emission_factor),
    }
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()

def _hmac_signature(signature: str) -> str | None:
    secret = os.environ.get("FEATURE_STORE_HMAC_SECRET")
    if not secret:
        import warnings
        warnings.warn(
            "[SECURITY] FEATURE_STORE_HMAC_SECRET env var is not set. "
            "Cache integrity HMAC verification is disabled. Set this variable in production.",
            stacklevel=2,
        )
        return None
    return hmac.new(secret.encode("utf-8"), signature.encode("utf-8"), hashlib.sha256).hexdigest()

def _file_hash(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _cache_paths(cache_dir: str) -> Dict[str, Path]:
    root = Path(cache_dir)
    root.mkdir(parents=True, exist_ok=True)
    return {
        "meta": root / "feature_store_meta.json",
        "production_raw": root / "production_raw.csv.gz",
        "process_timeseries_raw": root / "process_timeseries_raw.csv.gz",
        "process_summary_raw": root / "process_summary_raw.csv.gz",
        "features": root / "features.csv.gz",
    }


def _store_exists(paths: Dict[str, Path]) -> bool:
    return all(p.exists() for key, p in paths.items() if key != "meta") and paths["meta"].exists()


def load_or_build_pipeline(
    production_file: str,
    process_file: str,
    emission_factor: float,
    cache_dir: str = "artifacts/feature_store",
    use_store: bool = True,
    force_rebuild: bool = False,
) -> Tuple[PipelineArtifacts, dict]:
    paths = _cache_paths(cache_dir)
    signature = _build_signature(production_file, process_file, emission_factor)

    if use_store and not force_rebuild and _store_exists(paths):
        try:
            with paths["meta"].open("r", encoding="utf-8") as fh:
                meta = json.load(fh)
            if meta.get("signature") == signature:
                sig_hmac = meta.get("signature_hmac")
                expected_hmac = _hmac_signature(signature)
                if expected_hmac is None or sig_hmac == expected_hmac:
                    file_hashes = meta.get("file_hashes", {})
                    hashes_ok = (
                        file_hashes.get("production_raw") == _file_hash(paths["production_raw"])
                        and file_hashes.get("process_timeseries_raw") == _file_hash(paths["process_timeseries_raw"])
                        and file_hashes.get("process_summary_raw") == _file_hash(paths["process_summary_raw"])
                        and file_hashes.get("features") == _file_hash(paths["features"])
                    )
                    if hashes_ok:
                        artifacts = PipelineArtifacts(
                            production_raw=_read_cached_frame(paths["production_raw"]),
                            process_timeseries_raw=_read_cached_frame(paths["process_timeseries_raw"]),
                            process_summary_raw=_read_cached_frame(paths["process_summary_raw"]),
                            features=_read_cached_frame(paths["features"]),
                            cleaning_report=meta.get("cleaning_report", {}),
                        )
                        info = {
                            "cache_hit": True,
                            "cache_dir": str(Path(cache_dir).resolve()),
                            "signature": signature[:12],
                            "generated_at_utc": meta.get("generated_at_utc"),
                        }
                        log_event("feature_store_cache_hit", {"cache_dir": str(Path(cache_dir).resolve()), "signature": signature[:12]})
                        return artifacts, info
        except Exception:
            pass
        log_event("feature_store_cache_mismatch", {"cache_dir": str(Path(cache_dir).resolve())})

    artifacts = run_pipeline(
        production_file=production_file,
        process_file=process_file,
        emission_factor=emission_factor,
    )

    if use_store:
        artifacts.production_raw.to_csv(paths["production_raw"], index=False, compression="gzip")
        artifacts.process_timeseries_raw.to_csv(paths["process_timeseries_raw"], index=False, compression="gzip")
        artifacts.process_summary_raw.to_csv(paths["process_summary_raw"], index=False, compression="gzip")
        artifacts.features.to_csv(paths["features"], index=False, compression="gzip")

        meta = {
            "version": CACHE_VERSION,
            "signature": signature,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "cleaning_report": artifacts.cleaning_report,
            "signature_hmac": _hmac_signature(signature),
            "file_hashes": {
                "production_raw": _file_hash(paths["production_raw"]),
                "process_timeseries_raw": _file_hash(paths["process_timeseries_raw"]),
                "process_summary_raw": _file_hash(paths["process_summary_raw"]),
                "features": _file_hash(paths["features"]),
            },
        }
        with paths["meta"].open("w", encoding="utf-8") as fh:
            json.dump(meta, fh, indent=2)

    info = {
        "cache_hit": False,
        "cache_dir": str(Path(cache_dir).resolve()),
        "signature": signature[:12],
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    log_event("feature_store_cache_rebuilt", {"cache_dir": str(Path(cache_dir).resolve()), "signature": signature[:12]})
    return artifacts, info
