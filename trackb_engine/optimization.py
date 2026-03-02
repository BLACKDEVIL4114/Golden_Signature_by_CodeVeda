"""Multi-objective optimization with weighted scoring and Pareto filtering."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import numpy as np
import pandas as pd

from .config import OBJECTIVE_DIRECTIONS, PRIMARY_OBJECTIVES


@dataclass
class OptimizationTargets:
    min_yield: float
    min_quality: float
    max_energy: float
    max_carbon: float
    min_eco_score: float | None = None


class MultiObjectiveOptimizer:
    def __init__(self, feature_table: pd.DataFrame):
        self.feature_table = feature_table.copy()
        self.objectives = PRIMARY_OBJECTIVES
        self.directions = OBJECTIVE_DIRECTIONS

    def _normalize_objectives(self, df: pd.DataFrame) -> pd.DataFrame:
        normalized = pd.DataFrame(index=df.index)
        for obj in self.objectives:
            s = pd.to_numeric(df[obj], errors="coerce").astype(float)
            min_v = float(self.feature_table[obj].min())
            max_v = float(self.feature_table[obj].max())
            if np.isclose(max_v - min_v, 0.0):
                norm = pd.Series(np.full(len(s), 0.5), index=s.index)
            else:
                norm = (s - min_v) / (max_v - min_v)
            if self.directions[obj] == "min":
                norm = 1.0 - norm
            normalized[obj] = norm.clip(0.0, 1.0)
        return normalized

    def apply_targets(self, df: pd.DataFrame, targets: OptimizationTargets) -> pd.DataFrame:
        filtered = df[
            (df["Yield_Percent"] >= targets.min_yield)
            & (df["Quality_Score"] >= targets.min_quality)
            & (df["Total_Energy_kWh"] <= targets.max_energy)
            & (df["Carbon_kg"] <= targets.max_carbon)
        ].copy()
        if targets.min_eco_score is not None and "Eco_Efficiency_Score" in filtered.columns:
            filtered = filtered[filtered["Eco_Efficiency_Score"] >= float(targets.min_eco_score)].copy()
        return filtered

    def rank_batches(
        self,
        weights: Dict[str, float],
        targets: OptimizationTargets | None = None,
    ) -> pd.DataFrame:
        candidate_df = self.feature_table.copy()
        if targets is not None:
            candidate_df = self.apply_targets(candidate_df, targets)
            if candidate_df.empty:
                candidate_df = self.feature_table.copy()

        norm = self._normalize_objectives(candidate_df)
        denominator = float(sum(weights.get(obj, 0.0) for obj in self.objectives))
        if np.isclose(denominator, 0.0):
            denominator = 1.0

        score = pd.Series(np.zeros(len(norm)), index=norm.index, dtype=float)
        for obj in self.objectives:
            score += norm[obj] * float(weights.get(obj, 0.0))
        score = (score / denominator) * 100.0

        ranked = candidate_df.copy()
        ranked["Scenario_Score"] = score.round(2)
        ranked = ranked.sort_values("Scenario_Score", ascending=False).reset_index(drop=True)
        return ranked

    def score_candidate(self, candidate: pd.Series | dict, weights: Dict[str, float]) -> float:
        if isinstance(candidate, dict):
            candidate = pd.Series(candidate)

        weighted_sum = 0.0
        denominator = 0.0
        for obj in self.objectives:
            v = float(candidate[obj])
            min_v = float(self.feature_table[obj].min())
            max_v = float(self.feature_table[obj].max())
            if np.isclose(max_v - min_v, 0.0):
                norm = 0.5
            else:
                norm = (v - min_v) / (max_v - min_v)
            if self.directions[obj] == "min":
                norm = 1.0 - norm
            norm = float(np.clip(norm, 0.0, 1.0))
            w = float(weights.get(obj, 0.0))
            weighted_sum += norm * w
            denominator += w

        if np.isclose(denominator, 0.0):
            return 0.0
        return round((weighted_sum / denominator) * 100.0, 2)

    def pareto_front(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        data = self.feature_table if df is None else df
        if data.empty:
            return data.copy()

        # PERF: Cap at 300 candidates using a quick proxy sort so we keep
        # the most interesting ones even after capping.
        if len(data) > 300:
            _proxy = (
                data["Yield_Percent"] / (data["Yield_Percent"].max() + 1e-9)
                - data["Total_Energy_kWh"] / (data["Total_Energy_kWh"].max() + 1e-9)
            )
            data = data.loc[_proxy.nlargest(300).index].copy()

        # Fully vectorised numpy broadcasting — no Python loop at all.
        # Minimisation form: negate maximised objectives so "lower is better" everywhere.
        obj = np.column_stack([
            -data["Yield_Percent"].to_numpy(dtype=float),
            -data["Quality_Score"].to_numpy(dtype=float),
            -data["Performance_Score"].to_numpy(dtype=float),
            data["Total_Energy_kWh"].to_numpy(dtype=float),
            data["Carbon_kg"].to_numpy(dtype=float),
            -data["Eco_Efficiency_Score"].to_numpy(dtype=float),
        ])  # shape: (n, 6)

        # diff[i, j, k] = obj[j, k] - obj[i, k]  — shape (n, n, 6)
        diff = obj[np.newaxis, :, :] - obj[:, np.newaxis, :]
        all_leq = np.all(diff <= 0, axis=2)   # j dominates on every axis
        any_lt  = np.any(diff <  0, axis=2)   # j is strictly better on >=1 axis
        dominated_by = all_leq & any_lt
        np.fill_diagonal(dominated_by, False)
        is_dominated = dominated_by.any(axis=1)

        pareto = data.loc[~is_dominated].copy()
        pareto = pareto.sort_values(
            ["Total_Energy_kWh", "Yield_Percent"], ascending=[True, False]
        ).reset_index(drop=True)
        return pareto

