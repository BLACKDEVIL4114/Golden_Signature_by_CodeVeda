"""Real surrogate-model based continuous learning for Track B.

Replaces the previous random-noise simulation with a proper sklearn
GradientBoostingRegressor trained on historical batch data.
The surrogate predicts optimal parameter adjustments from the golden profile.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
try:
    from scipy.optimize import minimize
    _SCIPY_AVAILABLE = True
except Exception:
    _SCIPY_AVAILABLE = False

# Try to import sklearn - gracefully fall back to noise if not installed
try:
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    _SKLEARN_AVAILABLE = True
except ImportError:
    _SKLEARN_AVAILABLE = False

from .config import CONTROL_PARAMETERS, PRIMARY_OBJECTIVES


# ── Surrogate model registry (one model per KPI target) ────────────────────────
_SURROGATE_MODELS: dict = {}
_SURROGATE_SCALERS: dict = {}
_SURROGATE_TRAINED = False


def train_surrogate_models(features: pd.DataFrame) -> bool:
    """
    Train GradientBoosting surrogate models on historical batch data.

    Models learn:
      Control parameters (inputs) → Objective KPIs (outputs)

    Returns True if training succeeded, False if sklearn unavailable / data too small.
    """
    global _SURROGATE_MODELS, _SURROGATE_SCALERS, _SURROGATE_TRAINED

    if not _SKLEARN_AVAILABLE:
        return False

    input_cols = [c for c in CONTROL_PARAMETERS if c in features.columns]
    if len(input_cols) < 2 or len(features) < 30:
        # Not enough data to train meaningful surrogates
        return False

    X = features[input_cols].copy()
    for col in input_cols:
        X[col] = pd.to_numeric(X[col], errors="coerce")
    X = X.fillna(X.median(numeric_only=True))

    _SURROGATE_MODELS = {}
    _SURROGATE_SCALERS = {}

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    _SURROGATE_SCALERS["input"] = scaler
    _SURROGATE_SCALERS["input_cols"] = input_cols

    for target in PRIMARY_OBJECTIVES:
        if target not in features.columns:
            continue
        y = pd.to_numeric(features[target], errors="coerce").fillna(features[target].median())
        if y.nunique() < 5:
            continue
        try:
            model = GradientBoostingRegressor(
                n_estimators=80,
                max_depth=3,
                learning_rate=0.1,
                subsample=0.8,
                random_state=42,
            )
            model.fit(X_scaled, y)
            _SURROGATE_MODELS[target] = model
        except Exception:
            continue

    _SURROGATE_TRAINED = len(_SURROGATE_MODELS) >= 2
    return _SURROGATE_TRAINED


def _surrogate_improved_candidate(
    golden_profile: dict,
    emission_factor: float,
) -> pd.Series:
    """
    Use trained surrogate models to suggest a genuinely improved candidate.

    Strategy: perturb control parameters in the direction that the surrogate
    predicts will increase yield/quality and decrease energy/carbon.
    """
    input_cols: list[str] = _SURROGATE_SCALERS.get("input_cols", [])
    scaler: StandardScaler = _SURROGATE_SCALERS.get("input", None)

    if not input_cols or scaler is None:
        return _random_improved_candidate(golden_profile, emission_factor)

    # Build base control vector from golden profile
    base_controls = np.array([
        float(golden_profile.get(col, 0.0)) for col in input_cols
    ], dtype=float)

    best_candidate = dict(golden_profile)
    best_composite = _composite_score(golden_profile)

    # Hill-climb: try small perturbations and keep improvements
    rng = np.random.default_rng(seed=int(datetime.now().timestamp()) % 100000)
    for _ in range(40):
        # Small random perturbation ±5% of each parameter
        perturbation = rng.uniform(-0.05, 0.05, size=len(base_controls))
        candidate_controls = base_controls * (1.0 + perturbation)
        candidate_controls_scaled = scaler.transform(candidate_controls.reshape(1, -1))

        candidate = dict(golden_profile)
        for i, col in enumerate(input_cols):
            candidate[col] = round(float(candidate_controls[i]), 3)

        # Predict KPIs using surrogates
        for target, model in _SURROGATE_MODELS.items():
            predicted = float(model.predict(candidate_controls_scaled)[0])
            candidate[target] = round(predicted, 3)

        # Recompute derived values
        candidate["Carbon_kg"] = round(
            candidate.get("Total_Energy_kWh", golden_profile.get("Total_Energy_kWh", 0.0)) * float(emission_factor), 3
        )
        eco = (
            0.40 * (candidate.get("Yield_Percent", 0.0) / 100.0)
            + 0.15 * (candidate.get("Quality_Score", 0.0) / 100.0)
            + 0.20 * max(0.0, 1.0 - candidate.get("Total_Energy_kWh", 1.0) / max(1.0, golden_profile.get("Total_Energy_kWh", 1.0)))
            + 0.15 * max(0.0, 1.0 - candidate.get("Carbon_kg", 1.0) / max(1.0, golden_profile.get("Carbon_kg", 1.0)))
            + 0.10 * max(0.0, 1.0 - candidate.get("Drying_Time", 1.0) / max(1.0, golden_profile.get("Drying_Time", 1.0)))
        ) * 100.0
        candidate["Eco_Efficiency_Score"] = round(max(0.0, min(100.0, eco)), 2)

        composite = _composite_score(candidate)
        if composite > best_composite:
            best_composite = composite
            best_candidate = dict(candidate)

    best_candidate["Batch_ID"] = f"SIM_ML_{datetime.now().strftime('%H%M%S')}"
    best_candidate["Green_Zone"] = (
        "Green" if best_candidate.get("Eco_Efficiency_Score", 0) >= 70
        else "Yellow" if best_candidate.get("Eco_Efficiency_Score", 0) >= 40
        else "Red"
    )
    return pd.Series(best_candidate)

def _surrogate_optimized_candidate(
    golden_profile: dict,
    emission_factor: float,
) -> pd.Series:
    input_cols: list[str] = _SURROGATE_SCALERS.get("input_cols", [])
    scaler: StandardScaler = _SURROGATE_SCALERS.get("input", None)
    if not input_cols or scaler is None or not _SCIPY_AVAILABLE:
        return _surrogate_improved_candidate(golden_profile, emission_factor)
    base = np.array([float(golden_profile.get(col, 0.0)) for col in input_cols], dtype=float)
    lb = base * 0.9
    ub = base * 1.1
    def _objective(x: np.ndarray) -> float:
        xs = scaler.transform(x.reshape(1, -1))
        candidate = dict(golden_profile)
        for i, col in enumerate(input_cols):
            candidate[col] = float(x[i])
        for target, model in _SURROGATE_MODELS.items():
            candidate[target] = float(model.predict(xs)[0])
        if "Total_Energy_kWh" not in candidate:
            candidate["Total_Energy_kWh"] = float(golden_profile.get("Total_Energy_kWh", 0.0))
        candidate["Carbon_kg"] = float(candidate.get("Total_Energy_kWh", 0.0)) * float(emission_factor)
        return -float(_composite_score(candidate))
    bounds = [(float(lb[i]), float(ub[i])) for i in range(len(base))]
    res = minimize(_objective, base, bounds=bounds, method="L-BFGS-B")
    x_opt = res.x if res.success else base
    xs = scaler.transform(x_opt.reshape(1, -1))
    candidate = dict(golden_profile)
    for i, col in enumerate(input_cols):
        candidate[col] = round(float(x_opt[i]), 3)
    for target, model in _SURROGATE_MODELS.items():
        candidate[target] = round(float(model.predict(xs)[0]), 3)
    candidate["Carbon_kg"] = round(float(candidate.get("Total_Energy_kWh", golden_profile.get("Total_Energy_kWh", 0.0))) * float(emission_factor), 3)
    candidate["Batch_ID"] = f"SIM_OPT_{datetime.now().strftime('%H%M%S')}"
    candidate["Green_Zone"] = (
        "Green" if candidate.get("Eco_Efficiency_Score", 0) >= 70
        else "Yellow" if candidate.get("Eco_Efficiency_Score", 0) >= 40
        else "Red"
    )
    return pd.Series(candidate)

def _composite_score(profile: dict) -> float:
    """Simple composite score for hill-climbing direction."""
    return (
        0.30 * profile.get("Yield_Percent", 0.0)
        + 0.25 * profile.get("Quality_Score", 0.0)
        - 0.20 * profile.get("Total_Energy_kWh", 0.0) / 1000.0
        - 0.15 * profile.get("Carbon_kg", 0.0) / 100.0
        + 0.10 * profile.get("Eco_Efficiency_Score", 0.0)
    )


def _random_improved_candidate(
    golden_profile: dict,
    emission_factor: float,
    random_seed: int = 42,
) -> pd.Series:
    """Fallback: random perturbation when sklearn is unavailable or data is insufficient."""
    rng = np.random.default_rng(seed=random_seed + int(datetime.now().timestamp()) % 1000)
    candidate = dict(golden_profile)
    candidate["Batch_ID"] = f"SIM_{datetime.now().strftime('%H%M%S')}"
    candidate["Yield_Percent"] = round(float(golden_profile.get("Yield_Percent", 85.0)) + rng.uniform(0.2, 1.2), 2)
    candidate["Quality_Score"] = round(float(golden_profile.get("Quality_Score", 70.0)) + rng.uniform(0.4, 1.8), 2)
    candidate["Performance_Score"] = round(float(golden_profile.get("Performance_Score", 60.0)) + rng.uniform(0.3, 1.5), 2)
    energy_multiplier = 1.0 - rng.uniform(0.025, 0.09)
    candidate["Total_Energy_kWh"] = round(float(golden_profile.get("Total_Energy_kWh", 100.0)) * energy_multiplier, 3)
    candidate["Carbon_kg"] = round(candidate["Total_Energy_kWh"] * float(emission_factor), 3)
    candidate["Drying_Time"] = round(max(1.0, float(golden_profile.get("Drying_Time", 10.0)) - rng.uniform(0.5, 2.5)), 2)
    candidate["Drying_Temp"] = round(float(golden_profile.get("Drying_Temp", 80.0)) - rng.uniform(0.2, 1.8), 2)
    candidate["Machine_Speed"] = round(float(golden_profile.get("Machine_Speed", 200.0)) + rng.uniform(1.0, 8.0), 2)
    eco = (
        0.40 * (candidate["Yield_Percent"] / 100.0)
        + 0.15 * (candidate["Quality_Score"] / 100.0)
        + 0.20 * max(0.0, 1.0 - candidate["Total_Energy_kWh"] / max(1.0, float(golden_profile.get("Total_Energy_kWh", 1.0))))
        + 0.15 * max(0.0, 1.0 - candidate["Carbon_kg"] / max(1.0, float(golden_profile.get("Carbon_kg", 1.0))))
        + 0.10 * max(0.0, 1.0 - candidate["Drying_Time"] / max(1.0, float(golden_profile.get("Drying_Time", candidate["Drying_Time"]))))
    ) * 100.0
    candidate["Eco_Efficiency_Score"] = round(max(0.0, min(100.0, eco)), 2)
    candidate["Green_Zone"] = "Green" if candidate["Eco_Efficiency_Score"] >= 70 else "Yellow" if candidate["Eco_Efficiency_Score"] >= 40 else "Red"
    return pd.Series(candidate)


def simulate_improved_candidate(
    golden_profile: dict | pd.Series,
    emission_factor: float,
    random_seed: int = 42,
    feature_table: Optional[pd.DataFrame] = None,
) -> pd.Series:
    """
    Generate a simulated improved candidate batch.

    If a feature_table is provided and sklearn is available, trains and uses
    a GradientBoosting surrogate model for genuine ML-backed optimization.
    Falls back to random perturbation if sklearn is unavailable.
    """
    global _SURROGATE_TRAINED

    if isinstance(golden_profile, pd.Series):
        golden_profile = golden_profile.to_dict()

    if feature_table is not None and _SKLEARN_AVAILABLE:
        _SURROGATE_TRAINED = False
        train_surrogate_models(feature_table)

    if _SURROGATE_TRAINED:
        return _surrogate_optimized_candidate(golden_profile, emission_factor)
    else:
        return _random_improved_candidate(golden_profile, emission_factor, random_seed)
