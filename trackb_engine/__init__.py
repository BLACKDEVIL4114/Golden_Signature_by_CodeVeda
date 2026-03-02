"""Track B Optimization Engine package."""

from .config import DEFAULT_SCENARIOS, OBJECTIVE_DIRECTIONS, PRIMARY_OBJECTIVES
from .adapters import (
    build_manual_standard_row,
    build_scada_snapshot_row,
    build_standard_from_engine_features,
    normalize_to_standard_schema,
    standard_row_to_engine_candidate,
)
from .data_pipeline import PipelineArtifacts, run_pipeline
from .feature_store import load_or_build_pipeline
from .golden import GoldenSignatureManager
from .learning import simulate_improved_candidate
from .optimization import MultiObjectiveOptimizer
from .realtime import compare_batch_to_signature, estimate_roi, generate_adaptive_recommendations

__all__ = [
    "DEFAULT_SCENARIOS",
    "OBJECTIVE_DIRECTIONS",
    "PRIMARY_OBJECTIVES",
    "build_manual_standard_row",
    "build_scada_snapshot_row",
    "build_standard_from_engine_features",
    "normalize_to_standard_schema",
    "standard_row_to_engine_candidate",
    "PipelineArtifacts",
    "load_or_build_pipeline",
    "GoldenSignatureManager",
    "MultiObjectiveOptimizer",
    "compare_batch_to_signature",
    "estimate_roi",
    "generate_adaptive_recommendations",
    "run_pipeline",
    "simulate_improved_candidate",
]
