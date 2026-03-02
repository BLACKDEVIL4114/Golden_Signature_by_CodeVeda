from __future__ import annotations
from trackb_engine.feature_store import load_or_build_pipeline

def run_cache_rebuild_job(production_file: str, process_file: str, emission_factor: float, cache_dir: str) -> dict:
    artifacts, info = load_or_build_pipeline(
        production_file=production_file,
        process_file=process_file,
        emission_factor=emission_factor,
        cache_dir=cache_dir,
        use_store=True,
        force_rebuild=True,
    )
    return info
