"""Real-time batch comparison and adaptive correction logic."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from .config import CONTROL_PARAMETERS, OBJECTIVE_DIRECTIONS, PRIMARY_OBJECTIVES


def _safe_float(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def sanitize_csv(df: pd.DataFrame) -> bytes:
    safe = df.copy()
    dangerous = ("=", "+", "-", "@", "\t")
    for col in safe.columns:
        series = safe[col]
        if series.dtype == object:
            safe[col] = series.astype(str).apply(
                lambda s: ("'" + s) if any(s.startswith(p) for p in dangerous) else s
            )
    return safe.to_csv(index=False).encode("utf-8")


def compare_batch_to_signature(current: pd.Series, golden_profile: dict | pd.Series) -> pd.DataFrame:
    if isinstance(golden_profile, pd.Series):
        golden_profile = golden_profile.to_dict()

    focus_columns: Iterable[str] = [
        *PRIMARY_OBJECTIVES,
        *CONTROL_PARAMETERS,
    ]
    rows = []
    for col in focus_columns:
        if col not in current.index or col not in golden_profile:
            continue
        current_value = _safe_float(current[col])
        golden_value = _safe_float(golden_profile[col])
        deviation = current_value - golden_value
        deviation_pct = 0.0 if np.isclose(golden_value, 0.0) else (deviation / golden_value) * 100.0

        direction = OBJECTIVE_DIRECTIONS.get(col)
        if direction == "max":
            status = "On/Above Target" if current_value >= golden_value else "Below Target"
        elif direction == "min":
            status = "On/Below Target" if current_value <= golden_value else "Above Target"
        else:
            tolerance = 0.05 * (abs(golden_value) + 1e-6)
            status = "Aligned" if abs(deviation) <= tolerance else "Adjust"

        rows.append(
            {
                "Metric": col,
                "Current": round(current_value, 3),
                "Golden": round(golden_value, 3),
                "Deviation": round(deviation, 3),
                "Deviation_%": round(deviation_pct, 2),
                "Status": status,
            }
        )

    return pd.DataFrame(rows)


def generate_adaptive_recommendations(current: pd.Series, golden_profile: dict | pd.Series) -> list[str]:
    """Generate adaptive recommendations using universal manufacturing language.
    Works for any industry: textile, food, automotive, electronics, pharma, chemicals, etc.
    Recommendations are based on deviation from the Golden Signature profile.
    """
    if isinstance(golden_profile, pd.Series):
        golden_profile = golden_profile.to_dict()

    recs: list[str] = []

    cur_energy = _safe_float(current.get("Total_Energy_kWh", 0.0))
    gold_energy = _safe_float(golden_profile.get("Total_Energy_kWh", 0.0))
    cur_quality = _safe_float(current.get("Quality_Score", 0.0))
    gold_quality = _safe_float(golden_profile.get("Quality_Score", 0.0))
    cur_yield = _safe_float(current.get("Yield_Percent", 0.0))
    gold_yield = _safe_float(golden_profile.get("Yield_Percent", 0.0))
    cur_eco = _safe_float(current.get("Eco_Efficiency_Score", 0.0))
    gold_eco = _safe_float(golden_profile.get("Eco_Efficiency_Score", 0.0))
    cur_speed = _safe_float(current.get("Machine_Speed", 0.0))
    gold_speed = _safe_float(golden_profile.get("Machine_Speed", 0.0))

    # ── Energy deviation guidance (industry-agnostic) ─────────────────────────
    if gold_energy > 0 and cur_energy > gold_energy * 1.05:
        energy_excess_pct = round(((cur_energy - gold_energy) / gold_energy) * 100, 1)

        # Heat/thermal process deviation
        heat_dur_key = next((k for k in ["Heat_Duration", "Drying_Time", "Cure_Time", "Cook_Duration"] if k in current.index and k in golden_profile), None)
        heat_temp_key = next((k for k in ["Heat_Temp", "Drying_Temp", "Cure_Temp", "Cook_Temp"] if k in current.index and k in golden_profile), None)

        if heat_dur_key:
            delta_dur = _safe_float(current.get(heat_dur_key, 0.0)) - _safe_float(golden_profile.get(heat_dur_key, 0.0))
            if delta_dur > 1:
                recs.append(f"Reduce {heat_dur_key.replace('_', ' ').lower()} by ~{min(6.0, round(delta_dur, 1))} minutes to cut excess energy ({energy_excess_pct}% above golden).")
        if heat_temp_key:
            delta_temp = _safe_float(current.get(heat_temp_key, 0.0)) - _safe_float(golden_profile.get(heat_temp_key, 0.0))
            if delta_temp > 1:
                recs.append(f"Lower {heat_temp_key.replace('_', ' ').lower()} by ~{min(5.0, round(delta_temp, 1))}°C to align with golden thermal profile.")

        # Machine speed vs energy — if speed is lower than golden but energy is higher, idle time is the problem
        if gold_speed > 0 and cur_speed < gold_speed * 0.95:
            recs.append("Machine speed is below the golden benchmark — check for idle/standby states consuming energy without output.")
        else:
            recs.append("Review non-productive machine states and idle phases — these often account for 10–20% of wasted energy.")

    # ── Quality deviation guidance (industry-agnostic) ────────────────────────
    if gold_quality > 0 and cur_quality < gold_quality * 0.97:
        quality_gap_pct = round(((gold_quality - cur_quality) / gold_quality) * 100, 1)
        recs.append(f"Quality score is {quality_gap_pct}% below golden benchmark.")

        # Process agent (binder/dye/adhesive/reagent) check
        agent_key = next((k for k in ["Process_Agent_Amount", "Binder_Amount", "Dye_Amount", "Adhesive_Amount"] if k in current.index and k in golden_profile), None)
        if agent_key and _safe_float(current.get(agent_key, 0.0)) < _safe_float(golden_profile.get(agent_key, 0.0)):
            recs.append(f"Increase {agent_key.replace('_', ' ').lower()} toward the golden level — insufficient process agent is a common cause of quality loss.")

        # Press/force check
        force_key = next((k for k in ["Press_Force", "Compression_Force", "Stamp_Force", "Clamp_Force"] if k in current.index and k in golden_profile), None)
        if force_key and _safe_float(current.get(force_key, 0.0)) < _safe_float(golden_profile.get(force_key, 0.0)):
            recs.append(f"Raise {force_key.replace('_', ' ').lower()} toward the golden level — sub-optimal force reduces product integrity.")

        # Moisture/humidity check
        moisture_key = next((k for k in ["Moisture_Level", "Moisture_Content", "Humidity_Percent"] if k in current.index and k in golden_profile), None)
        if moisture_key and _safe_float(current.get(moisture_key, 0.0)) > _safe_float(golden_profile.get(moisture_key, 0.0)) + 0.1:
            recs.append(f"Moisture level exceeds golden profile — tighten endpoint control for {moisture_key.replace('_', ' ').lower()} before next stage.")

    # ── Yield deviation guidance (industry-agnostic) ──────────────────────────
    if gold_yield > 0 and cur_yield < gold_yield * 0.98:
        yield_gap_pct = round(((gold_yield - cur_yield) / gold_yield) * 100, 1)
        cycle_key = next((k for k in ["Cycle_Time", "Granulation_Time", "Mix_Time", "Process_Time"] if k in current.index and k in golden_profile), None)
        if cycle_key and _safe_float(current.get(cycle_key, 0.0)) < _safe_float(golden_profile.get(cycle_key, 0.0)):
            recs.append(f"Yield is {yield_gap_pct}% below golden — {cycle_key.replace('_', ' ').lower()} is shorter than the golden profile. Increase it gradually to improve uniformity and output rate.")
        else:
            recs.append(f"Yield is {yield_gap_pct}% below golden — run a parameter comparison against the Golden Signature to isolate root cause.")

    # ── Carbon / Eco guidance (universal) ─────────────────────────────────────
    cur_carbon = _safe_float(current.get("Carbon_kg", 0.0))
    gold_carbon = _safe_float(golden_profile.get("Carbon_kg", 0.0))
    if gold_carbon > 0 and cur_carbon > gold_carbon * 1.05:
        recs.append("Carbon footprint is above the golden benchmark — shift high-load operations to greener energy windows or reduce total energy consumption.")
    if gold_eco > 0 and cur_eco < gold_eco * 0.95:
        recs.append("Eco-Efficiency Score dropped against the golden benchmark — prioritize energy reduction and cycle-time correction first.")

    if not recs:
        recs.append("Batch is performing close to the Golden Signature. Maintain current settings and continue monitoring for drift.")

    return recs



def estimate_roi(
    current_energy_kwh: float,
    golden_energy_kwh: float,
    energy_cost_per_kwh: float,
    annual_batches: int,
) -> dict:
    delta_energy = float(current_energy_kwh) - float(golden_energy_kwh)
    savings_per_batch = max(0.0, delta_energy * float(energy_cost_per_kwh))
    annual_savings = savings_per_batch * int(annual_batches)
    return {
        "delta_energy_kwh": round(delta_energy, 3),
        "savings_per_batch_usd": round(savings_per_batch, 2),
        "annual_savings_usd": round(annual_savings, 2),
    }
