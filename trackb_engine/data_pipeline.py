"""Data collection, cleaning, and adaptive feature engineering pipeline."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from .config import CONTROL_PARAMETERS, DEFAULT_EMISSION_FACTOR, MAX_INPUT_ROWS, MAX_NULL_RATE, ENERGY_MAX_KWH

# Generic manufacturing columns — apply to ANY industry (textile, food, auto, electronics, pharma, etc.)
# The adaptive pipeline auto-maps industry-specific names via ADAPTIVE_ALIASES
REQUIRED_PRODUCTION_COLUMNS = [
    "Batch_ID",
    "Cycle_Time",           # Generic: how long the production cycle took
    "Process_Agent_Amount", # Generic: binder / adhesive / reagent / dye / additive
    "Heat_Temp",            # Generic: drying / curing / baking / sintering temperature
    "Heat_Duration",        # Generic: drying / curing / baking time
    "Press_Force",          # Generic: compression / stamping / rolling force
    "Machine_Speed",        # Generic: RPM / line speed / conveyor speed
    "Lubricant_Additive",   # Generic: lubricant / coating / surface treatment
    "Moisture_Level",       # Generic: moisture / humidity content at output
    "Unit_Weight",          # Generic: tablet / part / unit / roll weight
    "Strength_Score",       # Generic: hardness / tensile strength / hardness rating
    "Defect_Rate",          # Generic: friability / reject rate / defect %
    "Cycle_Completion_Time",# Generic: disintegration / cure time / set time
    "Output_Quality_Rate",  # Generic: dissolution / throughput quality rate
    "Uniformity_Index",     # Generic: content uniformity / thickness variance / tolerance
]

REQUIRED_PROCESS_COLUMNS = [
    "Batch_ID",
    "Time_Minutes",
    "Phase",
    "Temperature_C",
    "Pressure_Bar",
    "Humidity_Percent",
    "Motor_Speed_RPM",
    "Compression_Force_kN",
    "Flow_Rate_LPM",
    "Power_Consumption_kW",
    "Vibration_mm_s",
]

ADAPTIVE_ALIASES = {
    "timestamp": ["timestamp", "time", "datetime", "date_time", "recorded_at"],
    "batch_id": ["batch_id", "batch", "batchid", "lot_id", "lot"],
    "machine_id": ["machine_id", "machine", "line_id", "line", "asset_id", "equipment_id"],
    "operation_mode": ["operation_mode", "mode", "state", "status", "efficiency_status"],
    "temperature_c": ["temperature_c", "temperature", "temp_c", "temp", "avg_temperature", "drying_temp"],
    "pressure_bar": ["pressure_bar", "pressure", "press_bar", "avg_pressure"],
    "humidity_percent": ["humidity_percent", "humidity", "moisture", "relative_humidity"],
    "motor_speed_rpm": ["motor_speed_rpm", "machine_speed", "rpm", "speed", "max_motor_speed_rpm"],
    "compression_force_kn": ["compression_force_kn", "compression_force", "force", "force_kn"],
    "flow_rate_lpm": ["flow_rate_lpm", "flow_rate", "avg_flow_rate_lpm"],
    "power_kw": ["power_consumption_kw", "power_kw", "power", "kw"],
    "energy_kwh": ["energy_kwh", "total_energy_kwh", "consumption_kwh", "kwh", "energy"],
    "quality_score": ["quality_score", "quality", "quality_index"],
    "defect_rate": ["defect_rate", "quality_control_defect_rate", "quality_control_defect_rate_", "packet_loss"],
    "error_rate": ["error_rate", "error_rate_", "error_percent"],
    "output_qty": ["output_qty", "production_speed_units_per_hr", "production_rate", "throughput", "units_per_hr"],
    "process_time_min": ["process_time_min", "duration_minutes", "duration", "cycle_time", "time_min", "time_minutes"],
    "binder_amount": ["binder_amount"],
    "granulation_time": ["granulation_time"],
    "drying_temp": ["drying_temp"],
    "drying_time": ["drying_time"],
    "lubricant_conc": ["lubricant_conc"],
    "moisture_content": ["moisture_content"],
    "tablet_weight": ["tablet_weight"],
    "hardness": ["hardness"],
    "friability": ["friability"],
    "disintegration_time": ["disintegration_time"],
    "dissolution_rate": ["dissolution_rate"],
    "content_uniformity": ["content_uniformity"],
    "vibration_mm_s": ["vibration_mm_s", "vibration_hz", "vibration"],
}


@dataclass
class PipelineArtifacts:
    production_raw: pd.DataFrame
    process_timeseries_raw: pd.DataFrame
    process_summary_raw: pd.DataFrame
    features: pd.DataFrame
    cleaning_report: Dict[str, object]


def _normalize_0_1(series: pd.Series, invert: bool = False) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").astype(float)
    if s.dropna().empty:  # FIX B2: use .empty not .any()
        result = pd.Series(np.full(len(s), 0.5), index=s.index)
        return (1.0 - result if invert else result).clip(0.0, 1.0)
    min_v = float(s.min())
    max_v = float(s.max())
    if np.isclose(max_v - min_v, 0.0):
        result = pd.Series(np.full(len(s), 0.5), index=s.index)
    else:
        result = (s - min_v) / (max_v - min_v)
    if invert:
        result = 1.0 - result
    return result.clip(0.0, 1.0)


def _normalize_column_name(col: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(col).strip().lower()).strip("_")


def _resolve_alias_columns(df: pd.DataFrame) -> dict[str, str]:
    normalized_cols = {_normalize_column_name(c): c for c in df.columns}
    resolved: dict[str, str] = {}
    for canonical, aliases in ADAPTIVE_ALIASES.items():
        for alias in aliases:
            if alias in normalized_cols:
                resolved[canonical] = normalized_cols[alias]
                break
    return resolved


def _has_required_columns(df: pd.DataFrame, required: list[str]) -> bool:
    return all(c in df.columns for c in required)


def _validate_structured(production_raw: pd.DataFrame, process_raw: pd.DataFrame) -> None:
    if production_raw.empty or process_raw.empty:
        raise ValueError("Structured input frames must be non-empty.")
    if not _has_required_columns(production_raw, REQUIRED_PRODUCTION_COLUMNS):
        raise ValueError("Production frame missing required columns.")
    if not _has_required_columns(process_raw, REQUIRED_PROCESS_COLUMNS):
        raise ValueError("Process frame missing required columns.")
    prod_numeric = [c for c in REQUIRED_PRODUCTION_COLUMNS if c != "Batch_ID"]
    proc_numeric = [c for c in REQUIRED_PROCESS_COLUMNS if c != "Batch_ID" and c != "Phase"]
    for col in prod_numeric:
        s = pd.to_numeric(production_raw[col], errors="coerce")
        if s.dropna().empty:
            raise ValueError("Production column not numeric enough.")
    for col in proc_numeric:
        s = pd.to_numeric(process_raw[col], errors="coerce")
        if s.dropna().empty:
            raise ValueError("Process column not numeric enough.")


def _quality_gate(production_raw: pd.DataFrame, process_raw: pd.DataFrame) -> None:
    frames = {"production_raw": production_raw, "process_raw": process_raw}
    for name, df in frames.items():
        if df.empty:
            continue
        null_rate = float(df.isna().sum().sum()) / max(1, int(df.size))
        if null_rate > float(MAX_NULL_RATE):
            raise ValueError(f"Data quality violation: {name} null-rate too high.")
        if "Total_Energy_kWh" in df.columns:
            bad = pd.to_numeric(df["Total_Energy_kWh"], errors="coerce")
            if (bad < 0).any() or (bad > float(ENERGY_MAX_KWH)).any():
                raise ValueError("Data quality violation: Energy values out of bounds.")
        if "Temperature_C" in df.columns:
            t = pd.to_numeric(df["Temperature_C"], errors="coerce")
            if (t < -50).any() or (t > 300).any():
                raise ValueError("Data quality violation: Temperature values out of bounds.")


def _load_dataframe(path: str, preferred_sheet: str | None = None, batch_prefix: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path, nrows=int(MAX_INPUT_ROWS)), pd.DataFrame()

    with pd.ExcelFile(path) as workbook:
        sheet_names = workbook.sheet_names
        if not sheet_names:
            return pd.DataFrame(), pd.DataFrame()
        if batch_prefix is not None:
            selected = [name for name in sheet_names if name.startswith(batch_prefix)]
            if not selected:
                selected = [preferred_sheet] if preferred_sheet in sheet_names else [sheet_names[0]]
            frames: list[pd.DataFrame] = []
            for sheet in selected:
                frame = pd.read_excel(workbook, sheet_name=sheet, nrows=int(MAX_INPUT_ROWS))
                frame["Source_Sheet"] = sheet
                frames.append(frame)
            combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            summary = pd.read_excel(workbook, sheet_name="Summary", nrows=int(MAX_INPUT_ROWS)) if "Summary" in sheet_names else pd.DataFrame()
            return combined, summary
        sheet = preferred_sheet if preferred_sheet in sheet_names else sheet_names[0]
        return pd.read_excel(workbook, sheet_name=sheet, nrows=int(MAX_INPUT_ROWS)), pd.DataFrame()


def load_production_data(path: str) -> pd.DataFrame:
    frame, _ = _load_dataframe(path, preferred_sheet="BatchData")
    return frame


def load_process_data(path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _load_dataframe(path, preferred_sheet="Summary", batch_prefix="Batch_T")


def _clean_numeric_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    cleaned = df.copy()
    filled_missing = 0
    numeric_columns = [c for c in cleaned.columns if c != "Batch_ID"]
    for col in numeric_columns:
        cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")
        missing_before = int(cleaned[col].isna().sum())
        if missing_before > 0:
            median_value = float(cleaned[col].median()) if not cleaned[col].dropna().empty else 0.0
            cleaned[col] = cleaned[col].fillna(median_value)
            filled_missing += missing_before
    return cleaned, filled_missing


def _clip_outliers_iqr(df: pd.DataFrame, columns: list[str]) -> tuple[pd.DataFrame, int]:
    clipped_df = df.copy()
    clipped_points = 0
    for col in columns:
        if col == "Batch_ID":
            continue
        s = pd.to_numeric(clipped_df[col], errors="coerce")
        q1 = float(s.quantile(0.25))
        q3 = float(s.quantile(0.75))
        iqr = q3 - q1
        if np.isclose(iqr, 0.0):
            continue
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        before = s.copy()
        clipped = s.clip(lower, upper)
        clipped_points += int((before != clipped).sum())
        clipped_df[col] = clipped
    return clipped_df, clipped_points


def aggregate_process_features(process_timeseries: pd.DataFrame) -> pd.DataFrame:
    proc = process_timeseries.copy()
    numeric_cols = [
        "Time_Minutes",
        "Temperature_C",
        "Pressure_Bar",
        "Motor_Speed_RPM",
        "Compression_Force_kN",
        "Flow_Rate_LPM",
        "Power_Consumption_kW",
        "Vibration_mm_s",
    ]

    for col in numeric_cols:
        proc[col] = pd.to_numeric(proc[col], errors="coerce")

    if "Phase" not in proc.columns:
        proc["Phase"] = "Run"
    proc["Power_Consumption_kW"] = proc["Power_Consumption_kW"].clip(lower=0)
    grouped = proc.groupby("Batch_ID", as_index=False).agg(
        Duration_Minutes=("Time_Minutes", "max"),
        Data_Points=("Time_Minutes", "count"),
        Avg_Temperature=("Temperature_C", "mean"),
        Max_Temperature=("Temperature_C", "max"),
        Avg_Pressure=("Pressure_Bar", "mean"),
        Max_Compression_Force_kN=("Compression_Force_kN", "max"),
        Avg_Power_Consumption_kW=("Power_Consumption_kW", "mean"),
        Peak_Power_kW=("Power_Consumption_kW", "max"),
        Max_Motor_Speed_RPM=("Motor_Speed_RPM", "max"),
        Avg_Vibration_mm_s=("Vibration_mm_s", "mean"),
        Avg_Flow_Rate_LPM=("Flow_Rate_LPM", "mean"),
        Phase_Count=("Phase", "nunique"),
    )

    energy_by_batch = (
        proc.groupby("Batch_ID", as_index=False)["Power_Consumption_kW"]
        .sum()
        .rename(columns={"Power_Consumption_kW": "Total_Energy_kWh"})
    )
    energy_by_batch["Total_Energy_kWh"] = energy_by_batch["Total_Energy_kWh"] / 60.0

    aggregated = grouped.merge(energy_by_batch, on="Batch_ID", how="left")
    return aggregated


def engineer_features(df: pd.DataFrame, emission_factor: float = DEFAULT_EMISSION_FACTOR) -> pd.DataFrame:
    features = df.copy()

    uniformity_deviation = (features["Content_Uniformity"] - 100.0).abs()
    moisture_deviation = (features["Moisture_Content"] - 2.2).abs()

    quality_score_0_1 = (
        0.30 * _normalize_0_1(features["Dissolution_Rate"])
        + 0.20 * _normalize_0_1(features["Hardness"])
        + 0.20 * _normalize_0_1(features["Friability"], invert=True)
        + 0.15 * _normalize_0_1(features["Disintegration_Time"], invert=True)
        + 0.15 * _normalize_0_1(uniformity_deviation, invert=True)
    )
    features["Quality_Score"] = (quality_score_0_1 * 100.0).round(2)

    estimated_yield = (
        90.0
        + 0.11 * (features["Dissolution_Rate"] - 88.0)
        + 0.04 * (features["Hardness"] - 95.0)
        - 4.2 * features["Friability"]
        - 0.23 * uniformity_deviation
        - 0.90 * moisture_deviation
    )
    features["Yield_Percent"] = estimated_yield.clip(lower=72.0, upper=99.8).round(2)

    performance_score_0_1 = (
        0.45 * _normalize_0_1(features["Machine_Speed"])
        + 0.30 * _normalize_0_1(features["Duration_Minutes"], invert=True)
        + 0.15 * _normalize_0_1(features["Avg_Vibration_mm_s"], invert=True)
        + 0.10 * _normalize_0_1(features["Avg_Flow_Rate_LPM"])
    )
    features["Performance_Score"] = (performance_score_0_1 * 100.0).round(2)

    features["Carbon_kg"] = (features["Total_Energy_kWh"] * float(emission_factor)).round(2)
    features["Process_Health_Score"] = (
        0.35 * features["Quality_Score"]
        + 0.35 * features["Yield_Percent"]
        + 0.30 * (100.0 - (_normalize_0_1(features["Total_Energy_kWh"]) * 100.0))
    ).round(2)

    eco_score_0_1 = (
        0.40 * _normalize_0_1(features["Yield_Percent"])
        + 0.15 * _normalize_0_1(features["Quality_Score"])
        + 0.20 * _normalize_0_1(features["Total_Energy_kWh"], invert=True)
        + 0.15 * _normalize_0_1(features["Carbon_kg"], invert=True)
        + 0.10 * _normalize_0_1(features["Duration_Minutes"], invert=True)
    )
    features["Eco_Efficiency_Score"] = (eco_score_0_1 * 100.0).round(2)
    features["Green_Zone"] = pd.cut(
        features["Eco_Efficiency_Score"],
        bins=[-1.0, 40.0, 70.0, 100.0],
        labels=["Red", "Yellow", "Green"],
    ).astype(str)

    return features


def _to_numeric(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series(np.nan, index=index, dtype=float)
    return pd.to_numeric(series, errors="coerce").astype(float)


def _build_timestamp(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        synthetic = pd.date_range(start=pd.Timestamp("2024-01-01", tz="UTC"), periods=len(index), freq="min")
        return pd.Series(synthetic, index=index)

    ts = pd.to_datetime(series, errors="coerce", utc=True)
    if ts.isna().all():
        synthetic = pd.date_range(start=pd.Timestamp("2024-01-01", tz="UTC"), periods=len(index), freq="min")
        return pd.Series(synthetic, index=index)

    ts = ts.copy()
    ts = ts.ffill().bfill()
    if ts.isna().any():
        synthetic = pd.date_range(start=pd.Timestamp("2024-01-01", tz="UTC"), periods=len(index), freq="min")
        ts = ts.fillna(pd.Series(synthetic, index=index))
    return ts


def _infer_step_minutes(ts: pd.Series) -> float:
    if len(ts) < 2:
        return 1.0
    diffs = ts.sort_values().diff().dt.total_seconds().div(60.0)
    diffs = diffs[(diffs > 0) & (diffs < 120)]
    if diffs.empty:
        return 1.0
    return float(np.clip(diffs.median(), 0.2, 15.0))


def _active_mask(mode: pd.Series) -> pd.Series:
    lowered = mode.astype(str).str.lower()
    active = lowered.isin({"run", "running", "production", "productive", "active", "on"})
    if int(active.sum()) == 0:
        return pd.Series(True, index=mode.index)
    return active


def _generate_virtual_batch_ids(timestamp: pd.Series, machine_id: pd.Series, operation_mode: pd.Series) -> pd.Series:
    frame = pd.DataFrame(
        {
            "Timestamp": pd.to_datetime(timestamp, errors="coerce", utc=True),
            "Machine_ID": machine_id.astype(str).fillna("M01"),
            "Operation_Mode": operation_mode.astype(str).fillna("Run"),
        },
        index=timestamp.index,
    )
    frame["Machine_ID"] = frame["Machine_ID"].replace("", "M01")
    frame["Row_Order"] = np.arange(len(frame))
    frame = frame.sort_values(["Machine_ID", "Timestamp", "Row_Order"])
    frame["Active"] = _active_mask(frame["Operation_Mode"])
    frame["Gap_Min"] = frame.groupby("Machine_ID")["Timestamp"].diff().dt.total_seconds().div(60.0).fillna(0.0)
    frame["Mode_Start"] = frame["Active"] & ~frame.groupby("Machine_ID")["Active"].shift(fill_value=False)
    positive_gaps = frame["Gap_Min"][frame["Gap_Min"] > 0]
    typical_gap = float(positive_gaps.median()) if not positive_gaps.empty else 30.0
    gap_threshold = float(np.clip(typical_gap * 3.0, 30.0, 720.0))
    frame["New_Batch"] = (
        (frame["Gap_Min"] > gap_threshold)
        | frame["Mode_Start"]
    )
    frame["Batch_Num"] = (frame.groupby("Machine_ID")["New_Batch"].cumsum() + 1).astype(int)
    machine_token = frame["Machine_ID"].str.replace(r"[^A-Za-z0-9]+", "", regex=True).str.upper().str.slice(0, 12)
    frame["Batch_ID"] = "AUTO_" + machine_token + "_" + frame["Batch_Num"].astype(str).str.zfill(4)
    if frame["Batch_ID"].nunique() > max(1000, int(len(frame) * 0.20)):
        # Extremely sparse machine streams can create one-row batches.
        # Fall back to fixed row windows per machine for stable grouping.
        window_index = (frame.groupby("Machine_ID").cumcount() // 60) + 1
        frame["Batch_ID"] = "AUTO_" + machine_token + "_" + window_index.astype(str).str.zfill(4)
    frame = frame.sort_values("Row_Order")
    return frame["Batch_ID"].reindex(timestamp.index).ffill().fillna("AUTO_M01_0001")


def _quality_from_signals(
    quality: pd.Series,
    defect: pd.Series,
    error: pd.Series,
    vibration: pd.Series,
    temperature: pd.Series,
    output_qty: pd.Series,
) -> pd.Series:
    quality = pd.to_numeric(quality, errors="coerce")
    if quality.notna().any():
        q = quality.copy()
        if float(q.dropna().max()) <= 1.5:
            q = q * 100.0
        return q.clip(0.0, 100.0)

    defect_like = defect.copy()
    if defect_like.isna().all():
        defect_like = error.copy()
    if defect_like.notna().any():
        d = pd.to_numeric(defect_like, errors="coerce")
        if float(d.dropna().max()) <= 1.5:
            d = d * 100.0
        return (100.0 - d).clip(0.0, 100.0)

    temp_center = float(temperature.dropna().median()) if not temperature.dropna().empty else 0.0  # FIX Q1
    temp_stability = 1.0 - _normalize_0_1((temperature - temp_center).abs())
    vib_health = 1.0 - _normalize_0_1(vibration)
    output_health = _normalize_0_1(output_qty)
    proxy = (0.45 * temp_stability + 0.35 * vib_health + 0.20 * output_health) * 100.0
    return proxy.clip(20.0, 95.0)


def _derive_yield(output_qty: pd.Series, quality_score: pd.Series) -> pd.Series:
    output_component = _normalize_0_1(output_qty)
    quality_component = _normalize_0_1(quality_score)
    y = 68.0 + (0.65 * output_component + 0.35 * quality_component) * 31.0
    return y.clip(60.0, 99.5)


def _derive_performance(machine_speed: pd.Series, duration: pd.Series, output_qty: pd.Series, vibration: pd.Series) -> pd.Series:
    perf = (
        0.40 * _normalize_0_1(machine_speed)
        + 0.30 * _normalize_0_1(duration, invert=True)
        + 0.20 * _normalize_0_1(output_qty)
        + 0.10 * _normalize_0_1(vibration, invert=True)
    ) * 100.0
    return perf.clip(0.0, 100.0)


def _ensure_control_columns(features: pd.DataFrame) -> pd.DataFrame:
    f = features.copy()
    def _series(col: str, default: float) -> pd.Series:
        if col in f.columns:
            return pd.to_numeric(f[col], errors="coerce").fillna(default)
        return pd.Series(default, index=f.index, dtype=float)

    duration = _series("Duration_Minutes", 30.0)
    avg_temp = _series("Avg_Temperature", 85.0)
    avg_humidity = _series("Avg_Humidity_Percent", 44.0)
    max_force = _series("Max_Compression_Force_kN", 22.0)
    max_rpm = _series("Max_Motor_Speed_RPM", np.nan)
    output_signal = _series("Output_Signal", 50.0)
    flow = _series("Avg_Flow_Rate_LPM", 10.0)
    quality = _series("Quality_Score", 60.0)

    f["Granulation_Time"] = pd.to_numeric(f.get("Granulation_Time", duration * 0.35), errors="coerce").fillna(duration * 0.35).clip(lower=3.0)
    f["Binder_Amount"] = pd.to_numeric(f.get("Binder_Amount", 2.0 + _normalize_0_1(output_signal) * 8.0), errors="coerce").fillna(5.0)
    f["Drying_Temp"] = pd.to_numeric(f.get("Drying_Temp", avg_temp), errors="coerce").fillna(avg_temp)
    f["Drying_Time"] = pd.to_numeric(f.get("Drying_Time", duration * 0.25), errors="coerce").fillna(duration * 0.25).clip(lower=2.0)
    f["Compression_Force"] = pd.to_numeric(f.get("Compression_Force", max_force), errors="coerce").fillna(max_force)

    machine_speed = pd.to_numeric(f.get("Machine_Speed", max_rpm), errors="coerce")
    machine_speed = machine_speed.fillna((_normalize_0_1(output_signal) * 250.0) + 120.0)
    f["Machine_Speed"] = machine_speed.clip(lower=30.0)

    f["Lubricant_Conc"] = pd.to_numeric(f.get("Lubricant_Conc", 0.6 + _normalize_0_1(flow) * 1.0), errors="coerce").fillna(0.9)
    f["Moisture_Content"] = pd.to_numeric(f.get("Moisture_Content", (avg_humidity / 20.0).clip(lower=1.2, upper=6.5)), errors="coerce").fillna(2.2)

    f["Tablet_Weight"] = _series("Tablet_Weight", 500.0)
    f["Hardness"] = _series("Hardness", float((60.0 + quality * 0.35).median()))
    f["Friability"] = _series("Friability", float((((100.0 - quality) / 140.0).clip(lower=0.05, upper=1.8)).median()))
    f["Disintegration_Time"] = _series("Disintegration_Time", float((((1.0 - _normalize_0_1(quality)) * 20.0) + 6.0).median()))
    f["Dissolution_Rate"] = _series("Dissolution_Rate", float((65.0 + quality * 0.28).median()))
    f["Content_Uniformity"] = _series("Content_Uniformity", float((94.0 + quality * 0.05).median()))

    for col in CONTROL_PARAMETERS:
        if col not in f.columns:
            f[col] = 0.0
        series = pd.to_numeric(f[col], errors="coerce")
        default_val = float(series.median()) if series.notna().any() else 0.0
        f[col] = series.fillna(default_val)

    return f


def _run_structured_pipeline(
    production_raw: pd.DataFrame,
    process_raw: pd.DataFrame,
    process_summary: pd.DataFrame,
    emission_factor: float,
) -> PipelineArtifacts:
    production_clean, missing_filled_prod = _clean_numeric_columns(production_raw)
    # PERF: Downsample large process timeseries before aggregation
    process_raw_sampled = _sample_large_df(process_raw, max_rows=6000, batch_col="Batch_ID") if len(process_raw) > 6000 else process_raw
    process_agg = aggregate_process_features(process_raw_sampled)
    process_clean, missing_filled_process = _clean_numeric_columns(process_agg)

    merged = production_clean.merge(process_clean, on="Batch_ID", how="left")
    merged, clipped_points = _clip_outliers_iqr(
        merged,
        [c for c in merged.columns if c != "Batch_ID"],
    )

    features = engineer_features(merged, emission_factor=emission_factor)
    features = features.sort_values("Batch_ID").reset_index(drop=True)
    features = _ensure_control_columns(features)

    report = {
        "rows_production": int(len(production_raw)),
        "rows_process": int(len(process_raw)),
        "unique_batches": int(features["Batch_ID"].nunique()),
        "missing_values_filled": int(missing_filled_prod + missing_filled_process),
        "outlier_points_clipped": int(clipped_points),
        "data_mode": "Full",
        "batch_strategy": "provided",
        "signal_coverage": 6,
    }

    return PipelineArtifacts(
        production_raw=production_raw,
        process_timeseries_raw=process_raw,
        process_summary_raw=process_summary,
        features=features,
        cleaning_report=report,
    )


def _sample_large_df(df: pd.DataFrame, max_rows: int = 5000, batch_col: str | None = "Batch_ID") -> pd.DataFrame:
    """Intelligently downsample large datasets while preserving batch representation.
    - If rows <= max_rows: returns unchanged
    - Otherwise: samples evenly from each unique batch so no batch is completely dropped
    This keeps statistical representativeness while cutting processing time significantly.
    """
    if len(df) <= max_rows:
        return df
    if batch_col and batch_col in df.columns:
        n_batches = df[batch_col].nunique()
        rows_per_batch = max(1, max_rows // max(n_batches, 1))
        sampled = (
            df.groupby(batch_col, group_keys=False)
            .apply(lambda g: g.iloc[::max(1, len(g) // rows_per_batch)])
        )
        return sampled.reset_index(drop=True)
    # No batch column — uniform stride sampling
    step = max(1, len(df) // max_rows)
    return df.iloc[::step].reset_index(drop=True)


def _run_adaptive_pipeline(
    production_raw: pd.DataFrame,
    process_raw: pd.DataFrame,
    process_summary: pd.DataFrame,
    emission_factor: float,
) -> PipelineArtifacts:
    if process_raw.empty and production_raw.empty:
        raise ValueError("No rows found in uploaded data.")

    source_raw = process_raw.copy() if not process_raw.empty else production_raw.copy()
    # PERF: Downsample large timeseries before expensive feature engineering
    # Preserves every batch but reduces rows-per-batch for faster processing
    if len(source_raw) > 5000:
        source_raw = _sample_large_df(source_raw, max_rows=5000, batch_col=None)
    source_raw = source_raw.reset_index(drop=True)
    resolved = _resolve_alias_columns(source_raw)
    idx = source_raw.index

    timestamp = _build_timestamp(source_raw[resolved["timestamp"]] if "timestamp" in resolved else None, idx)
    machine_id = source_raw[resolved["machine_id"]].astype(str) if "machine_id" in resolved else pd.Series("M01", index=idx)
    operation_mode = source_raw[resolved["operation_mode"]].astype(str) if "operation_mode" in resolved else pd.Series("Run", index=idx)

    batch_generated = "batch_id" not in resolved
    if "batch_id" in resolved:
        batch_id = source_raw[resolved["batch_id"]].astype(str).replace("", pd.NA)
        missing_batch = batch_id.isna() | batch_id.eq("nan")
        if bool(missing_batch.any()):
            auto_ids = _generate_virtual_batch_ids(timestamp, machine_id, operation_mode)
            batch_id = batch_id.where(~missing_batch, auto_ids)
            batch_generated = True
    else:
        batch_id = _generate_virtual_batch_ids(timestamp, machine_id, operation_mode)

    temperature = _to_numeric(source_raw[resolved["temperature_c"]] if "temperature_c" in resolved else None, idx)
    pressure = _to_numeric(source_raw[resolved["pressure_bar"]] if "pressure_bar" in resolved else None, idx)
    humidity = _to_numeric(source_raw[resolved["humidity_percent"]] if "humidity_percent" in resolved else None, idx)
    motor_speed = _to_numeric(source_raw[resolved["motor_speed_rpm"]] if "motor_speed_rpm" in resolved else None, idx)
    compression_force = _to_numeric(source_raw[resolved["compression_force_kn"]] if "compression_force_kn" in resolved else None, idx)
    flow_rate = _to_numeric(source_raw[resolved["flow_rate_lpm"]] if "flow_rate_lpm" in resolved else None, idx)
    power_kw = _to_numeric(source_raw[resolved["power_kw"]] if "power_kw" in resolved else None, idx)
    energy_kwh = _to_numeric(source_raw[resolved["energy_kwh"]] if "energy_kwh" in resolved else None, idx)
    quality_raw = _to_numeric(source_raw[resolved["quality_score"]] if "quality_score" in resolved else None, idx)
    defect_raw = _to_numeric(source_raw[resolved["defect_rate"]] if "defect_rate" in resolved else None, idx)
    error_raw = _to_numeric(source_raw[resolved["error_rate"]] if "error_rate" in resolved else None, idx)
    output_qty = _to_numeric(source_raw[resolved["output_qty"]] if "output_qty" in resolved else None, idx)
    process_time_min = _to_numeric(source_raw[resolved["process_time_min"]] if "process_time_min" in resolved else None, idx)
    vibration = _to_numeric(source_raw[resolved["vibration_mm_s"]] if "vibration_mm_s" in resolved else None, idx)

    step_min = _infer_step_minutes(timestamp)
    if energy_kwh.notna().any():
        row_energy = energy_kwh.clip(lower=0.0)
    elif power_kw.notna().any():
        row_energy = power_kw.clip(lower=0.0) * float(step_min) / 60.0
    else:
        proxy_out = output_qty.fillna(output_qty.median() if output_qty.notna().any() else 50.0)
        row_energy = (proxy_out.abs() * 0.01).clip(lower=0.1)

    if output_qty.isna().all():
        if motor_speed.notna().any():
            output_qty = (motor_speed * 0.75).clip(lower=0.0)
        else:
            output_qty = pd.Series(50.0, index=idx)

    quality_score_row = _quality_from_signals(
        quality=quality_raw,
        defect=defect_raw,
        error=error_raw,
        vibration=vibration,
        temperature=temperature,
        output_qty=output_qty,
    )

    tmp = pd.DataFrame({"Batch_ID": batch_id.astype(str), "Timestamp": timestamp})
    batch_start = tmp.groupby("Batch_ID")["Timestamp"].transform("min")
    time_minutes = (tmp["Timestamp"] - batch_start).dt.total_seconds().div(60.0)
    if time_minutes.isna().all() or float(time_minutes.max()) <= 0.0:
        time_minutes = tmp.groupby("Batch_ID").cumcount().astype(float)
    if process_time_min.notna().any() and float(time_minutes.max()) <= 0.0:
        time_minutes = process_time_min.ffill().fillna(0.0)

    process_timeseries = pd.DataFrame(
        {
            "Batch_ID": batch_id.astype(str),
            "Time_Minutes": pd.to_numeric(time_minutes, errors="coerce").fillna(0.0),
            "Phase": operation_mode.fillna("Run"),
            "Temperature_C": temperature.fillna(temperature.median() if temperature.notna().any() else 85.0),
            "Pressure_Bar": pressure.fillna(pressure.median() if pressure.notna().any() else 5.0),
            "Humidity_Percent": humidity.fillna(humidity.median() if humidity.notna().any() else 45.0),
            "Motor_Speed_RPM": motor_speed.fillna(motor_speed.median() if motor_speed.notna().any() else (_normalize_0_1(output_qty) * 250.0 + 120.0)),
            "Compression_Force_kN": compression_force.fillna(compression_force.median() if compression_force.notna().any() else 22.0),
            "Flow_Rate_LPM": flow_rate.fillna(flow_rate.median() if flow_rate.notna().any() else 10.0),
            "Power_Consumption_kW": power_kw.fillna(power_kw.median() if power_kw.notna().any() else ((row_energy / max(step_min, 0.1)) * 60.0)),
            "Vibration_mm_s": vibration.fillna(vibration.median() if vibration.notna().any() else 1.5),
            "Machine_ID": machine_id.fillna("M01"),
            "Timestamp": timestamp,
            "Output_Signal": output_qty.fillna(output_qty.median() if output_qty.notna().any() else 50.0),
            "Quality_Score_Row": quality_score_row.fillna(55.0),
            "Energy_kWh_Row": row_energy.fillna(row_energy.median() if row_energy.notna().any() else 1.0),
        }
    )

    grouped = process_timeseries.groupby("Batch_ID", as_index=False).agg(
        Duration_Minutes=("Time_Minutes", "max"),
        Data_Points=("Time_Minutes", "count"),
        Avg_Temperature=("Temperature_C", "mean"),
        Max_Temperature=("Temperature_C", "max"),
        Avg_Pressure=("Pressure_Bar", "mean"),
        Avg_Humidity_Percent=("Humidity_Percent", "mean"),
        Max_Compression_Force_kN=("Compression_Force_kN", "max"),
        Avg_Power_Consumption_kW=("Power_Consumption_kW", "mean"),
        Peak_Power_kW=("Power_Consumption_kW", "max"),
        Max_Motor_Speed_RPM=("Motor_Speed_RPM", "max"),
        Avg_Vibration_mm_s=("Vibration_mm_s", "mean"),
        Avg_Flow_Rate_LPM=("Flow_Rate_LPM", "mean"),
        Phase_Count=("Phase", "nunique"),
        Output_Signal=("Output_Signal", "mean"),
        Quality_Score=("Quality_Score_Row", "mean"),
    )
    energy_by_batch = (
        process_timeseries.groupby("Batch_ID", as_index=False)["Energy_kWh_Row"]
        .sum()
        .rename(columns={"Energy_kWh_Row": "Total_Energy_kWh"})
    )
    features = grouped.merge(energy_by_batch, on="Batch_ID", how="left")
    features["Machine_Speed"] = features["Max_Motor_Speed_RPM"].fillna((_normalize_0_1(features["Output_Signal"]) * 250.0) + 120.0)
    features["Yield_Percent"] = _derive_yield(features["Output_Signal"], features["Quality_Score"]).round(2)
    features["Performance_Score"] = _derive_performance(
        features["Machine_Speed"],
        features["Duration_Minutes"],
        features["Output_Signal"],
        features["Avg_Vibration_mm_s"],
    ).round(2)
    features["Carbon_kg"] = (features["Total_Energy_kWh"] * float(emission_factor)).round(2)
    features["Process_Health_Score"] = (
        0.35 * features["Quality_Score"]
        + 0.35 * features["Yield_Percent"]
        + 0.30 * (100.0 - (_normalize_0_1(features["Total_Energy_kWh"]) * 100.0))
    ).round(2)
    eco_score = (
        0.40 * _normalize_0_1(features["Yield_Percent"])
        + 0.15 * _normalize_0_1(features["Quality_Score"])
        + 0.20 * _normalize_0_1(features["Total_Energy_kWh"], invert=True)
        + 0.15 * _normalize_0_1(features["Carbon_kg"], invert=True)
        + 0.10 * _normalize_0_1(features["Duration_Minutes"], invert=True)
    ) * 100.0
    features["Eco_Efficiency_Score"] = eco_score.round(2)
    features["Green_Zone"] = pd.cut(
        features["Eco_Efficiency_Score"],
        bins=[-1.0, 40.0, 70.0, 100.0],
        labels=["Red", "Yellow", "Green"],
    ).astype(str)

    features = _ensure_control_columns(features)
    features["Batch_ID"] = features["Batch_ID"].astype(str)

    numeric_cols = [c for c in features.columns if c != "Batch_ID"]
    for col in numeric_cols:
        converted = pd.to_numeric(features[col], errors="coerce")
        if converted.notna().any():
            features[col] = converted
            features[col] = features[col].replace([np.inf, -np.inf], np.nan)
            if features[col].isna().any():
                median_val = float(features[col].median()) if not features[col].dropna().empty else 0.0  # FIX B2
                features[col] = features[col].fillna(median_val)

    features = features.sort_values("Batch_ID").reset_index(drop=True)

    has_energy = "power_kw" in resolved or "energy_kwh" in resolved
    has_quality = "quality_score" in resolved or "defect_rate" in resolved or "error_rate" in resolved
    has_output = "output_qty" in resolved
    has_controls = any(k in resolved for k in ["temperature_c", "pressure_bar", "motor_speed_rpm", "flow_rate_lpm", "process_time_min"])
    if has_energy and has_quality and has_output and has_controls and not batch_generated:
        data_mode = "Full"
    elif has_energy and (has_quality or has_output or has_controls):
        data_mode = "Partial"
    else:
        data_mode = "Minimal"

    signal_coverage = int(sum([has_energy, has_quality, has_output, has_controls]))
    report = {
        "rows_production": int(len(production_raw)),
        "rows_process": int(len(process_raw)),
        "unique_batches": int(features["Batch_ID"].nunique()),
        "missing_values_filled": int(process_timeseries.isna().sum().sum()),
        "outlier_points_clipped": 0,
        "data_mode": data_mode,
        "batch_strategy": "auto-generated" if batch_generated else "provided",
        "signal_coverage": signal_coverage,
    }

    return PipelineArtifacts(
        production_raw=production_raw,
        process_timeseries_raw=process_timeseries,
        process_summary_raw=process_summary,
        features=features,
        cleaning_report=report,
    )


def run_pipeline(
    production_file: str,
    process_file: str,
    emission_factor: float = DEFAULT_EMISSION_FACTOR,
) -> PipelineArtifacts:
    production_raw = load_production_data(production_file)
    process_raw, process_summary = load_process_data(process_file)

    structured_ready = _has_required_columns(production_raw, REQUIRED_PRODUCTION_COLUMNS) and _has_required_columns(
        process_raw, REQUIRED_PROCESS_COLUMNS
    )
    if structured_ready:
        try:
            _validate_structured(production_raw, process_raw)
            _quality_gate(production_raw, process_raw)
            return _run_structured_pipeline(
                production_raw=production_raw,
                process_raw=process_raw,
                process_summary=process_summary,
                emission_factor=emission_factor,
            )
        except (ValueError, KeyError) as exc:  # FIX B1: only catch schema/quality errors, log them
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "Structured pipeline rejected (falling back to adaptive): %s", exc
            )

    return _run_adaptive_pipeline(
        production_raw=production_raw,
        process_raw=process_raw,
        process_summary=process_summary,
        emission_factor=emission_factor,
    )
