from __future__ import annotations

from pathlib import Path
import hashlib
import logging
import os

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

alt.data_transformers.disable_max_rows()

st.set_page_config(
    page_title="Advanced Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _inject_advanced_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Sora:wght@400;600;700;800&display=swap');

        :root {
            --bg0: #061428;
            --bg1: #0c2342;
            --panel: rgba(9, 27, 51, 0.84);
            --panel-2: rgba(7, 23, 44, 0.92);
            --border: #2b5e8d;
            --text: #f7fbff;
            --muted: #c9d8e7;
            --accent: #00a86b;
        }

        html, body, [class*="css"] {
            font-family: "Sora", sans-serif;
            color: var(--text) !important;
        }

        .stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"] {
            background:
                radial-gradient(900px 520px at 8% -14%, rgba(0,168,107,0.16), transparent 54%),
                radial-gradient(1200px 680px at 88% -22%, rgba(126,216,255,0.13), transparent 58%),
                linear-gradient(180deg, var(--bg0) 0%, var(--bg1) 58%, #071a33 100%);
        }

        [data-testid="stHeader"] {
            background: linear-gradient(180deg, rgba(6,20,40,0.98), rgba(10,28,53,0.94));
            border-bottom: 1px solid var(--border);
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(8,24,46,0.98), rgba(10,31,58,0.98));
            border-right: 1px solid rgba(53, 97, 143, 0.88);
        }

        h1, h2, h3, p, label, span, small {
            color: var(--text) !important;
        }

        .adv-shell {
            border: 1px solid var(--border);
            background: linear-gradient(155deg, var(--panel) 0%, var(--panel-2) 100%);
            border-radius: 22px;
            padding: 18px 20px;
            box-shadow: 0 8px 28px rgba(0, 0, 0, 0.34), inset 0 0 0 1px rgba(126, 216, 255, 0.08);
            margin-bottom: 14px;
        }

        .adv-title {
            font-size: 1.4rem;
            font-weight: 700;
            letter-spacing: 0.3px;
        }

        .adv-sub {
            color: var(--muted) !important;
            font-size: 0.9rem;
            margin-top: 3px;
        }

        .kpi-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 12px;
            margin-top: 12px;
        }

        .kpi-card {
            border: 1px solid rgba(34, 201, 138, 0.38);
            border-radius: 14px;
            padding: 12px 14px;
            background: rgba(10, 31, 58, 0.75);
        }

        .kpi-label {
            color: var(--muted);
            font-size: 0.8rem;
        }

        .kpi-value {
            font-size: 1.55rem;
            font-weight: 700;
            line-height: 1.15;
            margin-top: 4px;
            color: #7ed8ff;
        }

        .score-wrap {
            display: flex;
            align-items: center;
            justify-content: center;
            margin-top: 6px;
            margin-bottom: 2px;
        }

        .score-ring {
            width: 170px;
            height: 170px;
            border-radius: 50%;
            display: grid;
            place-items: center;
            background: conic-gradient(var(--accent) calc(var(--score) * 1%), rgba(126, 216, 255, 0.16) 0);
            box-shadow: inset 0 0 0 1px rgba(126, 216, 255, 0.22), 0 0 24px rgba(0, 168, 107, 0.20);
        }

        .score-inner {
            width: 124px;
            height: 124px;
            border-radius: 50%;
            display: grid;
            place-items: center;
            background: rgba(8, 24, 46, 0.96);
            border: 1px solid rgba(34, 201, 138, 0.40);
            text-align: center;
        }

        .score-num {
            font-size: 2rem;
            font-weight: 800;
            color: #7ed8ff;
            line-height: 1;
        }

        .score-lbl {
            color: var(--muted);
            font-size: 0.72rem;
            letter-spacing: 0.4px;
            text-transform: uppercase;
            margin-top: 3px;
        }

        .stSelectbox [data-baseweb="select"] > div {
            background-color: rgba(17, 28, 49, 0.85) !important;
            border: 1px solid rgba(43, 94, 141, 0.85) !important;
        }

        .stMultiSelect [data-baseweb="select"] > div,
        .stNumberInput > div > div > input,
        .stTextInput > div > div > input {
            background-color: rgba(17, 28, 49, 0.88) !important;
            border: 1px solid rgba(43, 94, 141, 0.90) !important;
            color: #f7fbff !important;
        }

        .stMultiSelect [data-baseweb="tag"] {
            background: rgba(34, 201, 138, 0.20) !important;
            border: 1px solid rgba(34, 201, 138, 0.46) !important;
            color: #eafff6 !important;
        }

        .stSlider [data-baseweb="slider"] [role="slider"] {
            background: #22c98a !important;
            border: 2px solid #baf0db !important;
        }

        .stSlider [data-baseweb="slider"] > div > div {
            background: rgba(126, 216, 255, 0.30) !important;
        }

        .stDataFrame, .stTable {
            border: 1px solid rgba(43, 94, 141, 0.90) !important;
            border-radius: 12px !important;
            background: rgba(9, 27, 51, 0.72) !important;
        }

        [data-testid="stDataFrameResizable"] th {
            background: rgba(8, 24, 46, 0.96) !important;
            color: #dbe9f8 !important;
            border-bottom: 1px solid rgba(43, 94, 141, 0.95) !important;
        }

        [data-testid="stDataFrameResizable"] td {
            background: rgba(10, 31, 58, 0.74) !important;
            color: #f7fbff !important;
            border-bottom: 1px solid rgba(43, 94, 141, 0.38) !important;
        }

        .stCaption, [data-testid="stCaptionContainer"] {
            color: #bed3e8 !important;
        }

        .stAlert {
            background: rgba(10, 31, 58, 0.90) !important;
            color: #f7fbff !important;
            border: 1px solid rgba(43, 94, 141, 0.92) !important;
        }

        [data-testid="stVegaLiteChart"] {
            background: linear-gradient(180deg, rgba(8, 24, 46, 0.74), rgba(10, 31, 58, 0.68));
            border: 1px solid rgba(43, 94, 141, 0.92);
            border-radius: 16px;
            padding: 10px 10px 2px 10px;
            box-shadow: inset 0 0 0 1px rgba(126, 216, 255, 0.08);
        }

        .chart-note {
            margin-top: 8px;
            border: 1px solid rgba(43, 94, 141, 0.86);
            border-radius: 12px;
            background: rgba(9, 27, 51, 0.68);
            padding: 10px 12px;
            color: #dce8f5;
            font-size: 0.86rem;
            line-height: 1.45;
        }

        .chart-note b {
            color: #7ed8ff;
        }

        @media (max-width: 1024px) {
            .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        }

        @media (max-width: 640px) {
            .kpi-grid { grid-template-columns: 1fr; }
            .score-ring { width: 150px; height: 150px; }
            .score-inner { width: 108px; height: 108px; }
            .score-num { font-size: 1.7rem; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _load_features() -> pd.DataFrame:
    features = st.session_state.get("adv_features")
    if isinstance(features, pd.DataFrame) and not features.empty:
        return features.copy()

    fallbacks = [
        Path("artifacts/feature_store/features.csv.gz"),
        Path("artifacts/demo_features.csv"),
    ]
    for fallback in fallbacks:
        if fallback.exists():
            try:
                loaded = pd.read_csv(fallback, low_memory=False)
            except Exception:
                logger.exception("Failed to read analytics fallback file: %s", fallback)
                continue
            if not loaded.empty:
                return loaded
    return pd.DataFrame()


def _show_dataset_upload_fallback() -> None:
    st.warning("Please upload your dataset files to continue")
    st.caption("Required files: _h_batch_production_data.xlsx and _h_batch_process_data_copy.xlsx")

    up_col1, up_col2 = st.columns(2)
    with up_col1:
        production_upload = st.file_uploader(
            "Upload _h_batch_production_data.xlsx",
            type=["xlsx", "csv"],
            key="adv_prod_upload",
        )
    with up_col2:
        process_upload = st.file_uploader(
            "Upload _h_batch_process_data_copy.xlsx",
            type=["xlsx", "csv"],
            key="adv_proc_upload",
        )

    if production_upload is not None and process_upload is not None:
        uploads_dir = Path("artifacts/uploads")
        uploads_dir.mkdir(parents=True, exist_ok=True)
        try:
            prod_blob = production_upload.getvalue()
            proc_blob = process_upload.getvalue()
            prod_name = f"production_uploaded_{hashlib.sha256(prod_blob).hexdigest()[:12]}{Path(production_upload.name).suffix.lower() or '.xlsx'}"
            proc_name = f"process_uploaded_{hashlib.sha256(proc_blob).hexdigest()[:12]}{Path(process_upload.name).suffix.lower() or '.xlsx'}"
            prod_path = uploads_dir / prod_name
            proc_path = uploads_dir / proc_name
            prod_path.write_bytes(prod_blob)
            proc_path.write_bytes(proc_blob)
            st.session_state["uploaded_production_meta"] = {
                "name": production_upload.name,
                "path": str(prod_path.resolve()),
                "stamp": (str(prod_path.resolve()), int(prod_path.stat().st_mtime)),
            }
            st.session_state["uploaded_process_meta"] = {
                "name": process_upload.name,
                "path": str(proc_path.resolve()),
                "stamp": (str(proc_path.resolve()), int(proc_path.stat().st_mtime)),
            }
            st.success("Files uploaded. Go back to Main Dashboard to continue processing.")
        except Exception as exc:
            logger.exception("Failed to store uploaded fallback files")
            st.error("Unable to save uploaded files. Please try again.")


def _current_score(batch_id: str, ranked: pd.DataFrame | None, row: pd.Series) -> float:
    if (
        isinstance(ranked, pd.DataFrame)
        and not ranked.empty
        and "Scenario_Score" in ranked.columns
        and "Batch_ID" in ranked.columns
    ):
        hit = ranked.loc[ranked["Batch_ID"].astype(str) == str(batch_id)]
        if not hit.empty:
            return max(0.0, min(1.0, _safe_float(hit.iloc[0]["Scenario_Score"], 0.0)))

    eco = _safe_float(row.get("Eco_Efficiency_Score", 0.0)) / 100.0
    quality = _safe_float(row.get("Quality_Score", 0.0)) / 100.0
    yield_pc = _safe_float(row.get("Yield_Percent", 0.0)) / 100.0
    return max(0.0, min(1.0, 0.45 * eco + 0.3 * quality + 0.25 * yield_pc))


def _spark_bar(data: pd.DataFrame, x: str, y: str, title: str) -> alt.Chart:
    return (
        alt.Chart(data)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3, opacity=0.95)
        .encode(
            x=alt.X(x, axis=alt.Axis(labelColor="#c9d8e7", title=None)),
            y=alt.Y(y, axis=alt.Axis(labelColor="#c9d8e7", title=None)),
            color=alt.value("#00a86b"),
            tooltip=[x, y],
        )
        .properties(height=240, title=title)
        .configure_view(strokeOpacity=0)
        .configure_axis(
            gridColor="rgba(126, 216, 255, 0.18)",
            labelColor="#c9d8e7",
            tickColor="rgba(126, 216, 255, 0.15)",
        )
        .configure_title(color="#f7fbff", fontSize=16, anchor="start")
        .configure_legend(labelColor="#f7fbff", titleColor="#f7fbff")
        .configure(background="transparent")
    )


def _spark_line(data: pd.DataFrame, x: str, y: str, title: str, color: str = "#22c98a") -> alt.Chart:
    return (
        alt.Chart(data)
        .mark_line(point=True, strokeWidth=2.2, color=color)
        .encode(
            x=alt.X(x, axis=alt.Axis(labelColor="#c9d8e7", title=None)),
            y=alt.Y(y, axis=alt.Axis(labelColor="#c9d8e7", title=None)),
            tooltip=[x, y],
        )
        .properties(height=260, title=title)
        .configure_view(strokeOpacity=0)
        .configure_axis(
            gridColor="rgba(126, 216, 255, 0.18)",
            labelColor="#c9d8e7",
            tickColor="rgba(126, 216, 255, 0.15)",
        )
        .configure_title(color="#f7fbff", fontSize=16, anchor="start")
        .configure_legend(labelColor="#f7fbff", titleColor="#f7fbff")
        .configure(background="transparent")
    )


def _render_chart_note(title: str, body: str) -> None:
    st.markdown(
        f"""
        <div class="chart-note">
            <b>{title}</b><br>{body}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _delta_text(current: float, reference: float, lower_is_better: bool = False, unit: str = "") -> str:
    if reference == 0:
        return "No baseline available."
    delta = current - reference
    pct = (delta / abs(reference)) * 100.0
    better = delta < 0 if lower_is_better else delta > 0
    direction = "higher" if delta > 0 else "lower"
    if abs(delta) < 1e-9:
        return "On target."
    verdict = "better" if better else "worse"
    return f"{abs(delta):.2f}{unit} {direction} ({abs(pct):.1f}%) than benchmark, {verdict}."


def _resolve_benchmark(
    benchmark_name: str,
    frame: pd.DataFrame,
    golden: dict[str, object],
    metrics: list[str],
) -> dict[str, float]:
    if frame.empty:
        return {metric: 0.0 for metric in metrics}

    lower_is_better = {"Total_Energy_kWh", "Carbon_kg"}
    resolved: dict[str, float] = {}

    for metric in metrics:
        series = pd.to_numeric(frame.get(metric), errors="coerce").dropna()
        if series.empty:
            resolved[metric] = 0.0
            continue

        if benchmark_name == "Golden Profile":
            resolved[metric] = _safe_float(golden.get(metric), _safe_float(series.median()))
        elif benchmark_name == "Top 10% Batches":
            q = 0.1 if metric in lower_is_better else 0.9
            resolved[metric] = _safe_float(series.quantile(q))
        else:
            resolved[metric] = _safe_float(series.median())

    return resolved


def _zscore_flags(series: pd.Series, threshold: float) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    mean = numeric.mean()
    std = numeric.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series([False] * len(series), index=series.index)
    z = ((numeric - mean) / std).abs()
    return z >= threshold


def _safe_metric_list(frame: pd.DataFrame, candidates: list[str]) -> list[str]:
    present = [col for col in candidates if col in frame.columns]
    return present or [col for col in frame.columns if pd.api.types.is_numeric_dtype(frame[col])][:4]


if os.getenv("AUTH_ENABLED", "false").lower() == "true" and not st.session_state.get("auth_user"):
    st.warning("Please log in from the main dashboard to access Advanced Analytics.")
    st.page_link("app.py", label="Go to Login", icon=":material/login:")
    st.stop()


_inject_advanced_theme()

st.markdown(
    """
    <div class="adv-shell">
      <div class="adv-title">Advanced Production Intelligence</div>
      <div class="adv-sub">Clean high-detail view for process health, efficiency, and benchmark gap analysis.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

features = _load_features()
ranked = st.session_state.get("adv_ranked")
golden = st.session_state.get("adv_golden_profile", {})

if features.empty or "Batch_ID" not in features.columns:
    _show_dataset_upload_fallback()
    st.page_link("app.py", label="Go to Main Dashboard", icon=":material/home:")
    st.stop()

features = features.copy()
features["Batch_ID"] = features["Batch_ID"].astype(str)
features = features.sort_values("Batch_ID").reset_index(drop=True)

numeric_cols = [
    "Total_Energy_kWh",
    "Quality_Score",
    "Yield_Percent",
    "Carbon_kg",
    "Eco_Efficiency_Score",
    "Process_Health_Score",
    "Avg_Temperature",
    "Avg_Pressure",
    "Avg_Vibration_mm_s",
    "Duration_Minutes",
]
analysis_metrics = _safe_metric_list(features, numeric_cols)

batch_ids = features["Batch_ID"].tolist()
default_idx = 0
if st.session_state.get("adv_current_batch_id") in batch_ids:
    default_idx = batch_ids.index(st.session_state["adv_current_batch_id"])

selector_left, selector_mid, selector_right, selector_far = st.columns([1.15, 1, 0.95, 1.35])
with selector_left:
    selected_batch = st.selectbox("Batch", batch_ids, index=default_idx)
with selector_mid:
    lookback = st.slider("Lookback Batches", min_value=20, max_value=min(240, len(features)), value=min(80, len(features)), step=10)
with selector_right:
    benchmark = st.selectbox("Benchmark", ["Golden Profile", "Top 10% Batches", "Median Batch"], index=0)
with selector_far:
    z_limit = st.slider("Anomaly Sensitivity (Z)", min_value=1.2, max_value=3.5, value=2.2, step=0.1)

metric_defaults = analysis_metrics[:4] if len(analysis_metrics) >= 4 else analysis_metrics
selected_metrics = st.multiselect(
    "Metrics for Trend View",
    options=analysis_metrics,
    default=metric_defaults,
)
if not selected_metrics:
    selected_metrics = metric_defaults[:1]

st.session_state["adv_page_batch_id"] = selected_batch
row = features.loc[features["Batch_ID"] == selected_batch].iloc[0]
score = _current_score(selected_batch, ranked, row)

window = features.tail(lookback).copy().reset_index(drop=True)
window["Batch_Seq"] = np.arange(1, len(window) + 1)
window["Energy_Anomaly"] = _zscore_flags(window.get("Total_Energy_kWh", pd.Series(dtype=float)), z_limit)

energy = _safe_float(row.get("Total_Energy_kWh"))
quality = _safe_float(row.get("Quality_Score"))
yield_pc = _safe_float(row.get("Yield_Percent"))
eco = _safe_float(row.get("Eco_Efficiency_Score"))
carbon = _safe_float(row.get("Carbon_kg"))
health = _safe_float(row.get("Process_Health_Score"))

benchmark_profile = _resolve_benchmark(
    benchmark_name=benchmark,
    frame=window,
    golden=golden if isinstance(golden, dict) else {},
    metrics=["Total_Energy_kWh", "Quality_Score", "Yield_Percent", "Carbon_kg", "Eco_Efficiency_Score"],
)

anomaly_count = int(window["Energy_Anomaly"].sum()) if "Energy_Anomaly" in window.columns else 0
energy_mean = _safe_float(window.get("Total_Energy_kWh", pd.Series([0.0])).mean())

st.markdown(
    f"""
    <div class="adv-shell">
      <div class="kpi-grid">
        <div class="kpi-card"><div class="kpi-label">Batch Energy</div><div class="kpi-value">{energy:.2f} kWh</div></div>
        <div class="kpi-card"><div class="kpi-label">Quality Score</div><div class="kpi-value">{quality:.1f}</div></div>
        <div class="kpi-card"><div class="kpi-label">Yield</div><div class="kpi-value">{yield_pc:.2f}%</div></div>
        <div class="kpi-card"><div class="kpi-label">Carbon</div><div class="kpi-value">{carbon:.2f} kg</div></div>
        <div class="kpi-card"><div class="kpi-label">Process Health</div><div class="kpi-value">{health:.1f}</div></div>
        <div class="kpi-card"><div class="kpi-label">Energy Anomalies ({lookback})</div><div class="kpi-value">{anomaly_count}</div></div>
      </div>
    </div>
    <div class="adv-shell">
      <div class="score-wrap">
        <div class="score-ring" style="--score:{score * 100:.2f}">
          <div class="score-inner">
            <div>
              <div class="score-num">{score:.2f}</div>
              <div class="score-lbl">Batch Score</div>
            </div>
          </div>
        </div>
      </div>
      <div class="adv-sub" style="text-align:center">Eco Efficiency: {eco:.1f} | Benchmark: {benchmark}</div>
    </div>
    """,
    unsafe_allow_html=True,
)

energy_latest = _safe_float(window["Total_Energy_kWh"].iloc[-1]) if "Total_Energy_kWh" in window.columns else 0.0
energy_prev_mean = _safe_float(window["Total_Energy_kWh"].iloc[:-1].mean()) if len(window) > 1 and "Total_Energy_kWh" in window.columns else energy_latest
energy_var = _safe_float(window["Total_Energy_kWh"].std()) if "Total_Energy_kWh" in window.columns else 0.0
energy_msg = (
    f"Energy per batch for last {len(window)} batches. Latest is {energy_latest:.2f} kWh, "
    f"recent mean is {energy_prev_mean:.2f} kWh, and spread is {energy_var:.2f}. "
    f"{'Low spread means stable process behavior.' if energy_var < 4 else 'High spread means the process needs tighter control settings.'}"
)

trend = window[["Batch_Seq", *selected_metrics]].melt(
    "Batch_Seq", var_name="Metric", value_name="Value"
)
yield_quality_corr = _safe_float(window["Yield_Percent"].corr(window["Quality_Score"]), 0.0) if {"Yield_Percent", "Quality_Score"}.issubset(window.columns) else 0.0
corr_text = (
    "strongly aligned" if yield_quality_corr >= 0.6
    else "moderately aligned" if yield_quality_corr >= 0.3
    else "weakly aligned"
)
yield_last = _safe_float(window["Yield_Percent"].iloc[-1]) if "Yield_Percent" in window.columns else 0.0
quality_last = _safe_float(window["Quality_Score"].iloc[-1]) if "Quality_Score" in window.columns else 0.0
trend_msg = (
    f"Yield and quality movement over time. Current yield is {yield_last:.2f}% and quality is {quality_last:.2f}. "
    f"Correlation is {yield_quality_corr:.2f} ({corr_text}). "
    "If yield goes up while quality goes down, throughput is likely being pushed too aggressively."
)

top_left, top_right = st.columns([1.35, 1])
with top_left:
    st.altair_chart(
        _spark_bar(window, "Batch_Seq:Q", "Total_Energy_kWh:Q", "Batch Production Energy"),
        use_container_width=True,
    )
    _render_chart_note("What This Says", energy_msg)

with top_right:
    line = (
        alt.Chart(trend)
        .mark_line(point=True, strokeWidth=2.2)
        .encode(
            x=alt.X("Batch_Seq:Q", axis=alt.Axis(labelColor="#c9d8e7", title=None)),
            y=alt.Y("Value:Q", axis=alt.Axis(labelColor="#c9d8e7", title=None)),
            color=alt.Color(
                "Metric:N",
                scale=alt.Scale(range=["#22c98a", "#7ed8ff", "#ffc857", "#f18f01", "#6aa8ff", "#ff6f91"]),
                legend=alt.Legend(labelColor="#f7fbff", titleColor="#f7fbff"),
            ),
            tooltip=["Batch_Seq", "Metric", "Value"],
        )
        .properties(height=240, title="Multi-Metric Trend")
        .configure_view(strokeOpacity=0)
        .configure_axis(
            gridColor="rgba(126, 216, 255, 0.18)",
            labelColor="#c9d8e7",
            tickColor="rgba(126, 216, 255, 0.15)",
        )
        .configure_legend(labelColor="#f7fbff", titleColor="#f7fbff")
        .configure_title(color="#f7fbff", fontSize=16, anchor="start")
        .configure(background="transparent")
    )
    st.altair_chart(line, use_container_width=True)
    _render_chart_note("What This Says", trend_msg)

bottom_left, bottom_right = st.columns(2)
with bottom_left:
    if "Avg_Temperature" in features.columns:
        temp_mean = _safe_float(window["Avg_Temperature"].mean())
        temp_min = _safe_float(window["Avg_Temperature"].min())
        temp_max = _safe_float(window["Avg_Temperature"].max())
        temp_msg = (
            f"Temperature response by batch. Range is {temp_min:.2f}C to {temp_max:.2f}C with mean {temp_mean:.2f}C. "
            "A narrow band usually improves quality repeatability and avoids extra energy consumption."
        )
        st.altair_chart(
            _spark_bar(window, "Batch_Seq:Q", "Avg_Temperature:Q", "Temperature Response"),
            use_container_width=True,
        )
    else:
        eco_mean = _safe_float(window["Eco_Efficiency_Score"].mean())
        eco_latest = _safe_float(window["Eco_Efficiency_Score"].iloc[-1])
        temp_msg = (
            f"Eco efficiency by batch. Current is {eco_latest:.2f} and recent average is {eco_mean:.2f}. "
            "Use this to monitor sustainability consistency when temperature signal is unavailable."
        )
        st.altair_chart(
            _spark_bar(window, "Batch_Seq:Q", "Eco_Efficiency_Score:Q", "Eco Efficiency Distribution"),
            use_container_width=True,
        )
    _render_chart_note("What This Says", temp_msg)

with bottom_right:
    bench_energy = benchmark_profile["Total_Energy_kWh"]
    bench_quality = benchmark_profile["Quality_Score"]
    bench_yield = benchmark_profile["Yield_Percent"]
    bench_eco = benchmark_profile["Eco_Efficiency_Score"]
    compare = pd.DataFrame(
        {
            "Metric": ["Energy kWh", "Quality", "Yield", "Eco"],
            "Current": [energy, quality, yield_pc, eco],
            "Benchmark": [bench_energy, bench_quality, bench_yield, bench_eco],
        }
    )
    melted = compare.melt("Metric", var_name="Type", value_name="Value")
    compare_chart = (
        alt.Chart(melted)
        .mark_bar(opacity=0.9)
        .encode(
            x=alt.X("Metric:N", axis=alt.Axis(labelColor="#c9d8e7", title=None)),
            y=alt.Y("Value:Q", axis=alt.Axis(labelColor="#c9d8e7", title=None)),
            xOffset="Type:N",
            color=alt.Color(
                "Type:N",
                scale=alt.Scale(domain=["Current", "Benchmark"], range=["#00a86b", "#7ed8ff"]),
                legend=alt.Legend(labelColor="#f7fbff", titleColor="#f7fbff"),
            ),
            tooltip=["Metric", "Type", "Value"],
        )
        .properties(height=240, title="Current vs Benchmark")
        .configure_view(strokeOpacity=0)
        .configure_axis(
            gridColor="rgba(126, 216, 255, 0.18)",
            labelColor="#c9d8e7",
            tickColor="rgba(126, 216, 255, 0.15)",
        )
        .configure_legend(labelColor="#f7fbff", titleColor="#f7fbff")
        .configure_title(color="#f7fbff", fontSize=16, anchor="start")
        .configure(background="transparent")
    )
    st.altair_chart(compare_chart, use_container_width=True)
    compare_msg = (
        "Direct benchmark gap. "
        f"Energy: {_delta_text(energy, bench_energy, lower_is_better=True, unit=' kWh')} "
        f"Quality: {_delta_text(quality, bench_quality)} "
        f"Yield: {_delta_text(yield_pc, bench_yield, unit=' pts')} "
        f"Eco: {_delta_text(eco, bench_eco, unit=' pts')}"
    )
    _render_chart_note("What This Says", compare_msg)

low_left, low_right = st.columns(2)
with low_left:
    if {"Total_Energy_kWh", "Quality_Score", "Yield_Percent"}.issubset(window.columns):
        scatter = (
            alt.Chart(window)
            .mark_circle(size=110, opacity=0.82)
            .encode(
                x=alt.X("Total_Energy_kWh:Q", axis=alt.Axis(labelColor="#c9d8e7", title="Energy (kWh)")),
                y=alt.Y("Quality_Score:Q", axis=alt.Axis(labelColor="#c9d8e7", title="Quality")),
                color=alt.Color(
                    "Energy_Anomaly:N",
                    scale=alt.Scale(domain=[False, True], range=["#22c98a", "#ff6b6b"]),
                    legend=alt.Legend(labelColor="#f7fbff", titleColor="#f7fbff", title="Anomaly"),
                ),
                size=alt.Size("Yield_Percent:Q", legend=None),
                tooltip=["Batch_ID", "Total_Energy_kWh", "Quality_Score", "Yield_Percent", "Energy_Anomaly"],
            )
            .properties(height=260, title="Energy vs Quality (Bubble = Yield)")
            .configure_view(strokeOpacity=0)
            .configure_axis(
                gridColor="rgba(126, 216, 255, 0.18)",
                labelColor="#c9d8e7",
                tickColor="rgba(126, 216, 255, 0.15)",
            )
            .configure_legend(labelColor="#f7fbff", titleColor="#f7fbff")
            .configure_title(color="#f7fbff", fontSize=16, anchor="start")
            .configure(background="transparent")
        )
        st.altair_chart(scatter, use_container_width=True)
        _render_chart_note(
            "What This Says",
            f"Relationship between energy and quality with yield as bubble size. {anomaly_count} high-energy outlier(s) flagged with Z >= {z_limit:.1f}.",
        )

with low_right:
    corr_pool = [metric for metric in analysis_metrics if metric in window.columns]
    corr_pool = corr_pool[:8]
    corr_base = window[corr_pool].apply(pd.to_numeric, errors="coerce")
    corr = corr_base.corr(numeric_only=True).fillna(0.0)
    corr_long = (
        corr.rename_axis("Metric_A")
        .reset_index()
        .melt(id_vars="Metric_A", var_name="Metric_B", value_name="Corr")
    )
    heat = (
        alt.Chart(corr_long)
        .mark_rect(cornerRadius=4)
        .encode(
            x=alt.X("Metric_A:N", axis=alt.Axis(labelAngle=-30, labelColor="#c9d8e7", title=None)),
            y=alt.Y("Metric_B:N", axis=alt.Axis(labelColor="#c9d8e7", title=None)),
            color=alt.Color("Corr:Q", scale=alt.Scale(scheme="tealblues"), legend=alt.Legend(labelColor="#f7fbff", titleColor="#f7fbff")),
            tooltip=["Metric_A", "Metric_B", alt.Tooltip("Corr:Q", format=".2f")],
        )
        .properties(height=260, title="Metric Correlation Heatmap")
        .configure_view(strokeOpacity=0)
        .configure_axis(
            gridColor="rgba(126, 216, 255, 0.10)",
            labelColor="#c9d8e7",
            tickColor="rgba(126, 216, 255, 0.15)",
        )
        .configure_title(color="#f7fbff", fontSize=16, anchor="start")
        .configure(background="transparent")
    )
    st.altair_chart(heat, use_container_width=True)
    _render_chart_note(
        "What This Says",
        "Strong positive values indicate metrics that move together; negative values indicate trade-offs where one metric rises while another drops.",
    )

energy_control = window[["Batch_Seq", "Total_Energy_kWh", "Energy_Anomaly"]].copy()
energy_control["Mean"] = energy_mean
energy_control["Upper"] = energy_mean + (window["Total_Energy_kWh"].std(ddof=0) * z_limit)
energy_control["Lower"] = energy_mean - (window["Total_Energy_kWh"].std(ddof=0) * z_limit)

control_base = alt.Chart(energy_control).encode(
    x=alt.X("Batch_Seq:Q", axis=alt.Axis(labelColor="#c9d8e7", title="Batch Seq"))
)
control_chart = (
    (
        control_base.mark_line(color="#7ed8ff", strokeWidth=2.2).encode(
            y=alt.Y("Total_Energy_kWh:Q", axis=alt.Axis(labelColor="#c9d8e7", title="Energy (kWh)"))
        )
        + control_base.mark_rule(color="#22c98a", strokeDash=[6, 4]).encode(y="Mean:Q")
        + control_base.mark_rule(color="#ffb703", strokeDash=[5, 3]).encode(y="Upper:Q")
        + control_base.mark_rule(color="#ffb703", strokeDash=[5, 3]).encode(y="Lower:Q")
        + control_base.mark_circle(color="#ff6b6b", size=85).encode(
            y="Total_Energy_kWh:Q",
            opacity=alt.condition("datum.Energy_Anomaly", alt.value(1), alt.value(0)),
            tooltip=["Batch_Seq", "Total_Energy_kWh", "Energy_Anomaly"],
        )
    )
    .properties(height=250, title="Energy Control Chart")
    .configure_view(strokeOpacity=0)
    .configure_axis(
        gridColor="rgba(126, 216, 255, 0.18)",
        labelColor="#c9d8e7",
        tickColor="rgba(126, 216, 255, 0.15)",
    )
    .configure_title(color="#f7fbff", fontSize=16, anchor="start")
    .configure(background="transparent")
)
st.altair_chart(control_chart, use_container_width=True)
_render_chart_note(
    "What This Says",
    f"Control limits are built from the selected {lookback} batches using Z={z_limit:.1f}. Points above upper or below lower bands are potential special-cause events.",
)

if anomaly_count > 0:
    anomalies = (
        window.loc[window["Energy_Anomaly"], ["Batch_ID", "Total_Energy_kWh", "Quality_Score", "Yield_Percent", "Carbon_kg"]]
        .sort_values("Total_Energy_kWh", ascending=False)
        .head(10)
    )
    st.markdown("#### Flagged Batches (Energy Outliers)")
    st.dataframe(anomalies, use_container_width=True, hide_index=True)

st.caption("Tip: select a batch in the main dashboard first for fully synchronized context.")
