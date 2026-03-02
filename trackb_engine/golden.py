"""Golden signature creation, storage, and continuous updates."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from .config import CONTROL_PARAMETERS, PRIMARY_OBJECTIVES
from .optimization import MultiObjectiveOptimizer, OptimizationTargets


def _serialize_value(v):
    if isinstance(v, (np.integer, np.int64, np.int32)):
        return int(v)
    if isinstance(v, (np.floating, np.float64, np.float32)):
        return float(v)
    return v


class GoldenSignatureManager:
    def __init__(self, storage_path: str):
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict:
        if not self.storage_path.exists():
            return {}
        # FIX B5: handle corrupted/partial JSON files gracefully
        try:
            with self.storage_path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "Golden signature file is corrupted or unreadable (%s). Regenerating.",
                self.storage_path,
            )
            return {}

    def save(self, payload: dict) -> None:
        with self.storage_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)

    def generate_signatures(
        self,
        optimizer: MultiObjectiveOptimizer,
        scenarios: Dict[str, Dict[str, float]],
        targets: OptimizationTargets,
        top_n: int = 3,
    ) -> dict:
        signatures: dict = {}
        for scenario_name, weights in scenarios.items():
            ranked = optimizer.rank_batches(weights=weights, targets=targets)
            if ranked.empty:
                continue
            best = ranked.iloc[0]
            alternatives = ranked.head(top_n)

            profile_columns = ["Batch_ID"] + CONTROL_PARAMETERS + PRIMARY_OBJECTIVES + ["Green_Zone", "Scenario_Score"]
            profile = {
                k: _serialize_value(best[k])
                for k in profile_columns
                if k in best.index
            }

            signatures[scenario_name] = {
                "batch_id": str(best["Batch_ID"]),
                "score": float(best["Scenario_Score"]),
                "weights": {k: float(v) for k, v in weights.items()},
                "profile": profile,
                "alternatives": [
                    {
                        "batch_id": str(row["Batch_ID"]),
                        "score": float(row["Scenario_Score"]),
                    }
                    for _, row in alternatives.iterrows()
                ],
            }

        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "signatures": signatures,
        }
        self.save(payload)
        return payload

    def promote_if_better(
        self,
        payload: dict,
        scenario_name: str,
        candidate_profile: pd.Series | dict,
        candidate_score: float,
        source_tag: str,
    ) -> tuple[bool, dict]:
        if isinstance(candidate_profile, pd.Series):
            candidate_profile = candidate_profile.to_dict()
        signatures = payload.get("signatures", {})
        current = signatures.get(scenario_name)
        if current is None:
            return False, payload

        if float(candidate_score) <= float(current.get("score", 0.0)):
            return False, payload

        profile_columns = ["Batch_ID"] + CONTROL_PARAMETERS + PRIMARY_OBJECTIVES + ["Green_Zone"]
        profile = {
            k: _serialize_value(candidate_profile[k])
            for k in profile_columns
            if k in candidate_profile
        }
        profile["Scenario_Score"] = float(candidate_score)

        signatures[scenario_name] = {
            "batch_id": str(candidate_profile.get("Batch_ID", f"SIM_{datetime.now().strftime('%H%M%S')}")),
            "score": float(candidate_score),
            "weights": current.get("weights", {}),
            "profile": profile,
            "alternatives": current.get("alternatives", []),
            "promoted_from": source_tag,
            "promoted_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        payload["signatures"] = signatures
        payload["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        self.save(payload)
        return True, payload
