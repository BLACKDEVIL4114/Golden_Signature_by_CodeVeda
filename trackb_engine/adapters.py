"""Universal data adapters and standard schema normalization."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

import pandas as pd

from .config import DEFAULT_EMISSION_FACTOR, STANDARD_SCHEMA

CANONICAL_ALIASES: Dict[str, list[str]] = {
    "timestamp": ["timestamp", "time", "datetime", "date_time", "recorded_at"],
    "batch_id": ["batch_id", "batch", "batchid", "lot_id", "lot"],
    "temperature_c": ["temperature_c", "temperature", "temp_c", "temp", "avg_temperature", "drying_temp"],
    "pressure_bar": ["pressure_bar", "pressure", "press_bar", "avg_pressure"],
    "rpm": ["rpm", "motor_speed_rpm", "machine_speed", "speed", "max_motor_speed"],
    "energy_kwh": ["energy_kwh", "total_energy_kwh", "energy", "power_kwh", "power_consumption_kwh"],
    "yield_percent": ["yield_percent", "yield", "yield_pct", "yield_percentage"],
    "quality_score": ["quality_score", "quality", "quality_index"],
    "process_time_min": ["process_time_min", "duration_minutes", "process_time", "time_min", "duration"],
    "carbon_kg": ["carbon_kg", "co2_kg", "carbon", "emission_kg", "co2"],
}


def _normalize_column_name(col: str) -> str:
    return str(col).strip().lower().replace(" ", "_")


def _resolve_columns(df: pd.DataFrame) -> dict:
    normalized_cols = {_normalize_column_name(c): c for c in df.columns}
    resolved: dict = {}
    for canonical, aliases in CANONICAL_ALIASES.items():
        for alias in aliases:
            if alias in normalized_cols:
                resolved[canonical] = normalized_cols[alias]
                break
    return resolved


def normalize_to_standard_schema(
    df: pd.DataFrame,
    emission_factor: float = DEFAULT_EMISSION_FACTOR,
    source_tag: str = "unknown",
) -> pd.DataFrame:
    resolved = _resolve_columns(df)
    out = pd.DataFrame(index=df.index)

    for canonical in STANDARD_SCHEMA:
        if canonical in resolved:
            out[canonical] = df[resolved[canonical]]
        else:
            out[canonical] = pd.NA

    if out["batch_id"].isna().all():
        out["batch_id"] = [f"GEN_{i+1:04d}" for i in range(len(out))]
    out["batch_id"] = out["batch_id"].astype(str)

    if out["timestamp"].isna().all():
        now_iso = datetime.now(timezone.utc).isoformat()
        out["timestamp"] = [now_iso] * len(out)
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    out["timestamp"] = out["timestamp"].fillna(pd.Timestamp.now(tz="UTC"))

    numeric_cols = [c for c in STANDARD_SCHEMA if c not in ("timestamp", "batch_id")]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["energy_kwh"] = out["energy_kwh"].clip(lower=0)
    out["process_time_min"] = out["process_time_min"].clip(lower=0)
    if out["carbon_kg"].isna().any():
        out["carbon_kg"] = out["carbon_kg"].fillna(out["energy_kwh"] * float(emission_factor))

    for col in numeric_cols:
        if out[col].isna().any():
            valid = out[col].dropna()
            median_val = valid.median() if not valid.empty else pd.NA
            out[col] = out[col].fillna(0.0 if pd.isna(median_val) else float(median_val))

    out["source_tag"] = source_tag
    return out


def build_standard_from_engine_features(features: pd.DataFrame) -> pd.DataFrame:
    raw = pd.DataFrame(
        {
            "timestamp": pd.Timestamp.now(tz="UTC"),
            "batch_id": features["Batch_ID"],
            "temperature_c": features["Avg_Temperature"],
            "pressure_bar": features["Avg_Pressure"],
            "rpm": features["Machine_Speed"],
            "energy_kwh": features["Total_Energy_kWh"],
            "yield_percent": features["Yield_Percent"],
            "quality_score": features["Quality_Score"],
            "process_time_min": features["Duration_Minutes"],
            "carbon_kg": features["Carbon_kg"],
        }
    )
    return normalize_to_standard_schema(raw, source_tag="historical_excel_adapter")


def build_manual_standard_row(
    batch_id: str,
    temperature_c: float,
    pressure_bar: float,
    rpm: float,
    energy_kwh: float,
    yield_percent: float,
    quality_score: float,
    process_time_min: float,
    emission_factor: float = DEFAULT_EMISSION_FACTOR,
) -> pd.DataFrame:
    payload = pd.DataFrame(
        [
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "batch_id": batch_id,
                "temperature_c": temperature_c,
                "pressure_bar": pressure_bar,
                "rpm": rpm,
                "energy_kwh": energy_kwh,
                "yield_percent": yield_percent,
                "quality_score": quality_score,
                "process_time_min": process_time_min,
                "carbon_kg": energy_kwh * float(emission_factor),
            }
        ]
    )
    return normalize_to_standard_schema(payload, emission_factor=emission_factor, source_tag="manual_adapter")


def build_scada_snapshot_row(
    process_timeseries: pd.DataFrame,
    batch_id: str,
    emission_factor: float = DEFAULT_EMISSION_FACTOR,
) -> pd.DataFrame:
    proc = process_timeseries.copy()
    subset = proc.loc[proc["Batch_ID"] == batch_id].copy()
    if subset.empty:
        return normalize_to_standard_schema(pd.DataFrame([{"batch_id": batch_id}]), source_tag="scada_adapter")

    for col in [
        "Temperature_C",
        "Pressure_Bar",
        "Motor_Speed_RPM",
        "Power_Consumption_kW",
        "Time_Minutes",
    ]:
        subset[col] = pd.to_numeric(subset[col], errors="coerce")

    energy_kwh = subset["Power_Consumption_kW"].clip(lower=0).sum() / 60.0
    process_time = float(subset["Time_Minutes"].max())
    temp = float(subset["Temperature_C"].mean())
    pressure = float(subset["Pressure_Bar"].mean())
    rpm = float(subset["Motor_Speed_RPM"].mean())

    raw = pd.DataFrame(
        [
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "batch_id": batch_id,
                "temperature_c": temp,
                "pressure_bar": pressure,
                "rpm": rpm,
                "energy_kwh": energy_kwh,
                "yield_percent": pd.NA,
                "quality_score": pd.NA,
                "process_time_min": process_time,
                "carbon_kg": energy_kwh * float(emission_factor),
            }
        ]
    )
    return normalize_to_standard_schema(raw, emission_factor=emission_factor, source_tag="scada_adapter")


def standard_row_to_engine_candidate(
    standard_row: pd.Series,
    template_row: pd.Series,
    emission_factor: float,
) -> pd.Series:
    candidate = template_row.copy()
    candidate["Batch_ID"] = str(standard_row["batch_id"])
    candidate["Avg_Temperature"] = float(standard_row["temperature_c"])
    candidate["Avg_Pressure"] = float(standard_row["pressure_bar"])
    candidate["Machine_Speed"] = float(standard_row["rpm"])
    candidate["Duration_Minutes"] = float(standard_row["process_time_min"])
    candidate["Total_Energy_kWh"] = float(standard_row["energy_kwh"])
    candidate["Carbon_kg"] = float(standard_row["carbon_kg"])
    incoming_yield = float(standard_row["yield_percent"])
    incoming_quality = float(standard_row["quality_score"])
    candidate["Yield_Percent"] = float(template_row.get("Yield_Percent", 85.0)) if incoming_yield <= 0 else incoming_yield
    candidate["Quality_Score"] = float(template_row.get("Quality_Score", 55.0)) if incoming_quality <= 0 else incoming_quality

    if "Performance_Score" in candidate.index:
        speed_factor = float(standard_row["rpm"]) / max(1.0, float(template_row.get("Machine_Speed", 1.0)))
        time_factor = max(0.4, float(template_row.get("Duration_Minutes", 1.0)) / max(1.0, float(standard_row["process_time_min"])))
        perf = float(template_row.get("Performance_Score", 50.0)) * 0.5 * (speed_factor + time_factor)
        candidate["Performance_Score"] = max(0.0, min(100.0, perf))

    if "Eco_Efficiency_Score" in candidate.index:
        yield_norm = float(candidate["Yield_Percent"]) / 100.0
        quality_norm = float(candidate["Quality_Score"]) / 100.0
        energy_baseline = max(1.0, float(template_row.get("Total_Energy_kWh", candidate["Total_Energy_kWh"])))
        carbon_baseline = max(1.0, float(template_row.get("Carbon_kg", candidate["Carbon_kg"])))
        time_baseline = max(1.0, float(template_row.get("Duration_Minutes", candidate["Duration_Minutes"])))
        energy_penalty = min(2.0, float(candidate["Total_Energy_kWh"]) / energy_baseline)
        carbon_penalty = min(2.0, float(candidate["Carbon_kg"]) / carbon_baseline)
        time_penalty = min(2.0, float(candidate["Duration_Minutes"]) / time_baseline)

        eco = (0.25 * yield_norm + 0.15 * quality_norm + 0.30 * (1 - energy_penalty / 2) + 0.20 * (1 - carbon_penalty / 2) + 0.10 * (1 - time_penalty / 2)) * 100.0
        candidate["Eco_Efficiency_Score"] = max(0.0, min(100.0, eco))

    if "Green_Zone" in candidate.index:
        eco_score = float(candidate.get("Eco_Efficiency_Score", 50.0))
        if eco_score >= 70.0:
            candidate["Green_Zone"] = "Green"
        elif eco_score >= 40.0:
            candidate["Green_Zone"] = "Yellow"
        else:
            candidate["Green_Zone"] = "Red"

    if pd.isna(candidate.get("Carbon_kg", pd.NA)):
        candidate["Carbon_kg"] = float(candidate["Total_Energy_kWh"]) * float(emission_factor)

    return candidate
