from __future__ import annotations

import hashlib
import json as _json
import logging
from pathlib import Path

import altair as alt
alt.data_transformers.disable_max_rows()
import pandas as pd
import streamlit as st
import threading
import time
import smtplib
import os

from dotenv import load_dotenv
import streamlit_authenticator as stauth

from trackb_engine.config import (
    DEFAULT_ANNUAL_BATCHES,
    DEFAULT_EMISSION_FACTOR,
    DEFAULT_ENERGY_COST,
    DEFAULT_SCENARIOS,
    GOLDEN_SIGNATURE_FILE,
    FEATURE_STORE_DIR,
    FEATURE_STORE_UPLOADS_DIR,
    PROCESS_DATA_FILE,
    PRODUCTION_DATA_FILE,
    MAX_UPLOAD_BYTES,
    ALLOWED_UPLOAD_EXTS,
    AUTH_ENABLED,
    AUTH_USERS,
    UPLOAD_RATE_LIMIT,
    UPLOAD_RATE_WINDOW_SEC,
    BACKOFF_MAX_SECONDS,
    ALERT_ENABLED,
    ALERT_EMAIL_TO,
    ALERT_SMTP_SERVER,
    ALERT_SMTP_PORT,
    ALERT_SMTP_USER,
    ALERT_SMTP_PASS,
    ENERGY_MAX_KWH,
    MAX_PARETO_ROWS,
)
from trackb_engine.feature_store import load_or_build_pipeline
from trackb_engine.golden import GoldenSignatureManager
from trackb_engine.optimization import MultiObjectiveOptimizer, OptimizationTargets
from trackb_engine.realtime import compare_batch_to_signature, estimate_roi, generate_adaptive_recommendations
from trackb_engine.realtime import sanitize_csv
from trackb_engine.telemetry import log_event
try:
    from api import app as fastapi_app
except Exception:
    fastapi_app = None
app = fastapi_app

load_dotenv()
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Smart Pollution Monitoring System | Air, Noise, Water, Plastic Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)
Path("artifacts").mkdir(parents=True, exist_ok=True)
# Automatic cloud-safe mode: disable background worker on headless servers
_IS_HEADLESS = str(os.environ.get("STREAMLIT_SERVER_HEADLESS", "true")).lower() == "true"
_DISABLE_BG_WORKER = str(os.environ.get("AGPO_DISABLE_WORKER", "auto")).lower()
if _DISABLE_BG_WORKER == "auto":
    _DISABLE_BG_WORKER = _IS_HEADLESS

# English-only UI strings
_EN = {
    "Quick Panel": "Quick Panel",
    "Scan": "Scan",
    "Check": "Check",
    "Status": "Status",
    "Check Safety": "Check Safety",
    "SAFE": "SAFE",
    "RISK DETECTED": "RISK DETECTED",
    "Voice guidance": "Voice guidance",
    "Using sidebar to upload": "Use sidebar to upload files",
    "Comparison View": "Comparison View",
    "History Logs": "History Logs",
    "Admin Panel": "Admin Panel",
    "AI Confidence": "AI Confidence",
    "Avg Scenario Score": "Avg Scenario Score",
    "Score StdDev": "Score StdDev",
    "Users": "Users",
    "Security Events": "Security Events",
    "Download Manager CSV": "Download Manager CSV",
    "Download Executive PDF": "Download Executive PDF",
    "Block uploads in session": "Block uploads in session",
    "Uploads blocked": "Uploads blocked by admin",
}

def tr(key: str) -> str:
    """Returns the English UI string for the given key."""
    return _EN.get(key, key)

def speak(text: str) -> None:
    # FIX S2: encode text as a JSON string to prevent XSS via script injection
    safe_text = _json.dumps(str(text))
    st.markdown(
        f"""
        <script>
        try {{
          const u = new SpeechSynthesisUtterance({safe_text});
          u.lang = (window.lang_code || "en-US");
          speechSynthesis.speak(u);
        }} catch (e) {{}}
        </script>
        """,
        unsafe_allow_html=True,
    )

def inject_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Sora:wght@400;600;700;800&display=swap');

        :root {
            --bg0: #061428;
            --bg1: #0c2342;
            --panel: rgba(9, 27, 51, 0.86);
            --border: #2b5e8d;
            --text: #f7fbff;
            --muted: #c9d8e7;
            --emerald: #00a86b;
            --emerald-soft: #22c98a;
        }

        html, body, [class*="css"] {
            font-family: "Sora", sans-serif;
            color: var(--text) !important;
        }

        h1, h2, h3, h4, h5, h6, p, label, span, small {
            color: var(--text) !important;
        }

        .stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"] {
            background:
                radial-gradient(900px 520px at 8% -14%, rgba(0,168,107,0.18), transparent 54%),
                radial-gradient(1200px 680px at 88% -22%, rgba(126,216,255,0.14), transparent 58%),
                linear-gradient(180deg, var(--bg0) 0%, var(--bg1) 58%, #071a33 100%);
        }

        [data-testid="stHeader"] {
            background: linear-gradient(180deg, rgba(6,20,40,0.98), rgba(10,28,53,0.94));
            border-bottom: 1px solid var(--border);
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(8,24,46,0.98), rgba(10,31,58,0.98));
            border-right: 1px solid var(--border);
        }

        /* Streamlit top-right main menu popup (Rerun / Settings / Clear cache) */
        [data-testid="stMainMenuPopover"],
        [data-testid="stMainMenuPopover"] > div,
        [data-baseweb="popover"],
        [data-baseweb="popover"] > div,
        [data-baseweb="menu"],
        div[role="menu"],
        div[role="presentation"] {
            background: #0f2747 !important;
            border: 1px solid #35618f !important;
            border-radius: 12px !important;
            color: #f7fbff !important;
        }

        [data-testid="stMainMenuPopover"] *,
        [data-baseweb="popover"] *,
        [data-baseweb="menu"] *,
        div[role="menu"] *,
        div[role="presentation"] * {
            color: #f7fbff !important;
            -webkit-text-fill-color: #f7fbff !important;
            opacity: 1 !important;
            text-shadow: none !important;
            font-weight: 600 !important;
        }

        [data-testid="stMainMenuPopover"] ul,
        [data-testid="stMainMenuPopover"] li,
        [data-testid="stMainMenuPopover"] li > div,
        [data-testid="stMainMenuPopover"] li > button,
        [data-testid="stMainMenuPopover"] button,
        [data-testid="stMainMenuPopover"] a,
        [data-baseweb="popover"] ul,
        [data-baseweb="popover"] li,
        [data-baseweb="popover"] li > div,
        [data-baseweb="popover"] li > button,
        [data-baseweb="popover"] button,
        [data-baseweb="popover"] a,
        [data-baseweb="menu"] ul,
        [data-baseweb="menu"] li,
        [data-baseweb="menu"] li > div,
        [data-baseweb="menu"] li > button,
        [data-baseweb="menu"] button,
        [data-baseweb="menu"] a,
        div[role="menu"] ul,
        div[role="menu"] li,
        div[role="menu"] li > div,
        div[role="menu"] li > button,
        div[role="menu"] button,
        div[role="menu"] a,
        [role="menuitem"],
        li[role="menuitem"],
        button[role="menuitem"] {
            background: #0f2747 !important;
            background-color: #0f2747 !important;
            color: #f7fbff !important;
            -webkit-text-fill-color: #f7fbff !important;
            border-color: #2b4f74 !important;
        }

        [data-testid="stMainMenuPopover"] :is(div, button, a, li, ul, span, p, small, strong),
        [data-baseweb="popover"] :is(div, button, a, li, ul, span, p, small, strong),
        [data-baseweb="menu"] :is(div, button, a, li, ul, span, p, small, strong),
        div[role="menu"] :is(div, button, a, li, ul, span, p, small, strong) {
            background-color: #0f2747 !important;
            color: #f7fbff !important;
            -webkit-text-fill-color: #f7fbff !important;
        }

        [data-testid="stMainMenuPopover"] hr,
        [data-baseweb="popover"] hr,
        [data-baseweb="menu"] hr,
        div[role="menu"] hr {
            border-color: #2b4f74 !important;
            background-color: #2b4f74 !important;
        }

        [data-testid="stMainMenuPopover"] li:hover,
        [data-testid="stMainMenuPopover"] li > div:hover,
        [data-testid="stMainMenuPopover"] li > button:hover,
        [data-baseweb="popover"] li:hover,
        [data-baseweb="popover"] li > div:hover,
        [data-baseweb="popover"] li > button:hover,
        [role="menuitem"]:hover,
        li[role="menuitem"]:hover,
        button[role="menuitem"]:hover,
        [role="menuitem"]:focus-visible,
        li[role="menuitem"]:focus-visible,
        button[role="menuitem"]:focus-visible {
            background: #16365d !important;
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
        }

        [data-testid="stMainMenuPopover"] [aria-disabled="true"],
        [data-testid="stMainMenuPopover"] button:disabled,
        [data-baseweb="popover"] [aria-disabled="true"],
        [data-baseweb="popover"] button:disabled,
        [data-baseweb="menu"] [aria-disabled="true"],
        [data-baseweb="menu"] button:disabled,
        div[role="menu"] [aria-disabled="true"],
        div[role="menu"] button:disabled {
            background: #1a3a5b !important;
            color: #dce8f5 !important;
            -webkit-text-fill-color: #dce8f5 !important;
            opacity: 1 !important;
        }

        [data-testid="stSidebar"] * {
            color: var(--text) !important;
        }

        .block-container {
            max-width: 1380px;
            padding-top: 0.8rem;
            padding-bottom: 2.1rem;
        }

        [data-testid="stMetric"] {
            border: 1px solid var(--border);
            border-radius: 14px;
            background: linear-gradient(160deg, rgba(8, 32, 58, 0.95), rgba(6, 22, 40, 0.95));
            box-shadow: 0 12px 24px rgba(0,0,0,0.22);
            padding: 0.86rem 0.95rem;
        }

        [data-testid="stMetricLabel"], [data-testid="stMetricValue"], [data-testid="stMetricDelta"] {
            color: var(--text) !important;
        }

        [data-testid="stMetricLabel"] {
            color: #a7c2da !important;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-size: 0.72rem !important;
            font-weight: 700 !important;
        }

        [data-testid="stMetricValue"] {
            font-weight: 800 !important;
            letter-spacing: 0.01em;
        }

        [data-testid="stCaptionContainer"], .stCaption, .stMarkdown, .stText {
            color: var(--muted) !important;
        }

        .stButton > button {
            border: none;
            border-radius: 10px;
            color: #04261b;
            font-weight: 700;
            background: linear-gradient(135deg, var(--emerald), var(--emerald-soft));
        }

        [data-baseweb="select"] > div {
            background: #102b4a !important;
            border: 1px solid #2c547f !important;
        }

        [data-testid="stSelectbox"] label,
        [data-testid="stSelectbox"] div[role="combobox"],
        [data-testid="stSelectbox"] div[role="combobox"] * {
            color: var(--text) !important;
            -webkit-text-fill-color: var(--text) !important;
        }

        [data-baseweb="select"] *,
        [data-baseweb="select"] span,
        [data-baseweb="select"] input,
        [data-baseweb="select"] svg {
            color: var(--text) !important;
            fill: var(--text) !important;
            -webkit-text-fill-color: var(--text) !important;
            font-weight: 600 !important;
        }

        div[role="listbox"],
        div[role="listbox"] * {
            background: #0f2747 !important;
            color: #f7fbff !important;
            -webkit-text-fill-color: #f7fbff !important;
        }

        [data-testid="stNumberInput"] input, [data-testid="stTextInput"] input {
            background: #102b4a !important;
            color: #f7fbff !important;
            -webkit-text-fill-color: #f7fbff !important;
            font-weight: 600 !important;
        }

        [data-testid="stAlert"] * {
            color: var(--text) !important;
            font-weight: 600 !important;
        }

        
        </style>
        """,
        unsafe_allow_html=True,
    )


def _detect_single_file_candidate(path: str) -> bool:
    try:
        p = Path(path)
        if p.suffix.lower() == ".xlsx":
            df = pd.read_excel(p, nrows=200)
        else:
            df = pd.read_csv(p, nrows=200, encoding="utf-8", errors="ignore")
        cols = [str(c).lower() for c in df.columns]
        keys = ["time", "timestamp", "minute", "temperature", "motor", "rpm", "pressure", "flow"]
        hits = sum(1 for c in cols if any(k in c for k in keys))
        return hits >= 2
    except Exception:
        return False

def _data_check_summary(prod: pd.DataFrame, proc: pd.DataFrame) -> dict:
    rows_total = int(len(prod)) + int(len(proc))
    miss_prod = int(prod.isna().sum().sum()) if not prod.empty else 0
    miss_proc = int(proc.isna().sum().sum()) if not proc.empty else 0
    cells_total = int(prod.size) + int(proc.size)
    missing_pct = (100.0 * (miss_prod + miss_proc) / max(1, cells_total))
    # Out-of-range checks (simple, universal)
    bad_rows = 0
    if "Total_Energy_kWh" in prod.columns:
        e = pd.to_numeric(prod["Total_Energy_kWh"], errors="coerce")
        bad_rows += int(((e < 0) | (e > float(ENERGY_MAX_KWH))).sum())
    if "Temperature_C" in proc.columns:
        t = pd.to_numeric(proc["Temperature_C"], errors="coerce")
        bad_rows += int(((t < -50) | (t > 300)).sum())
    out_pct = (100.0 * bad_rows / max(1, rows_total))
    return {"rows": rows_total, "missing_pct": round(missing_pct, 2), "out_of_range_pct": round(out_pct, 2)}
def _stamp(path: str) -> tuple[int, int]:
    p = Path(path)
    stat = p.stat()
    return int(stat.st_mtime_ns), int(stat.st_size)


@st.cache_data(show_spinner=False)
def load_pipeline(
    production_file: str,
    process_file: str,
    cache_dir: str,
    emission_factor: float,
    use_feature_store: bool,
    force_rebuild: bool,
    prod_stamp: tuple[str, int],
    proc_stamp: tuple[str, int],
):
    _ = (prod_stamp, proc_stamp)
    return load_or_build_pipeline(
        production_file=production_file,
        process_file=process_file,
        emission_factor=emission_factor,
        cache_dir=cache_dir,
        use_store=use_feature_store,
        force_rebuild=force_rebuild,
    )


def persist_uploaded_file(uploaded_file, uploads_dir: Path, prefix: str) -> tuple[Path, tuple[str, int]]:
    blob = uploaded_file.getvalue()
    if not isinstance(blob, (bytes, bytearray)) or len(blob) == 0:
        raise ValueError("Uploaded file is empty.")
    if len(blob) > int(MAX_UPLOAD_BYTES):
        raise ValueError("Uploaded file too large.")
    digest = hashlib.sha256(blob).hexdigest()
    name = str(uploaded_file.name)
    if name.count(".") > 1:
        raise ValueError("Invalid filename: multiple dots not allowed.")
    ext = Path(name).suffix.lower() or ".xlsx"
    normalized = ext.replace(".", "")
    if normalized not in ALLOWED_UPLOAD_EXTS:
        raise ValueError("Unsupported file type.")
    if normalized == "xlsx":
        if not bytes(blob).startswith(b"PK"):
            raise ValueError("Invalid XLSX file content.")
    elif normalized == "csv":
        head = bytes(blob[:4096])
        try:
            head.decode("utf-8")
        except Exception:
            raise ValueError("CSV must be UTF-8 encoded.")
    out_path = (uploads_dir / f"{prefix}_{digest[:12]}{ext}").resolve()
    # SECURITY FIX: Prevent path traversal - ensure output stays inside uploads_dir
    if not str(out_path).startswith(str(uploads_dir.resolve())):
        raise ValueError("Path traversal attempt detected in filename.")
    if not out_path.exists():
        out_path.write_bytes(blob)
    return out_path, (digest[:16], len(blob))


@st.cache_data(show_spinner=False)
def build_targets(features: pd.DataFrame, strictness: str) -> OptimizationTargets:
    """Cached: quantile stats are expensive — only recompute when features or strictness changes."""
    if strictness == "Open":
        q = 0.20
    elif strictness == "Strict":
        q = 0.55
    else:
        q = 0.40
    return OptimizationTargets(
        min_yield=float(features["Yield_Percent"].quantile(q)),
        min_quality=float(features["Quality_Score"].quantile(q)),
        max_energy=float(features["Total_Energy_kWh"].quantile(1.0 - q + 0.15)),
        max_carbon=float(features["Carbon_kg"].quantile(1.0 - q + 0.15)),
        min_eco_score=float(features["Eco_Efficiency_Score"].quantile(q)),
    )


@st.cache_data(show_spinner=False)
def _cached_rank_batches(
    features: pd.DataFrame,
    weights_json: str,
    strictness: str,
) -> pd.DataFrame:
    """Cached wrapper around MultiObjectiveOptimizer.rank_batches.
    Called with JSON-serialised weights so Streamlit can hash the key.
    Only recomputes when features, weights, or strictness changes."""
    import json as _j
    weights = _j.loads(weights_json)
    optimizer = MultiObjectiveOptimizer(features)
    targets = build_targets(features, strictness)
    return optimizer.rank_batches(weights=weights, targets=targets)


@st.cache_data(show_spinner=False)
def _cached_compare_batch(current_json: str, golden_json: str) -> pd.DataFrame:
    """Cached batch-vs-golden comparison (only reruns when batch or golden changes)."""
    import json as _j
    current = pd.Series(_j.loads(current_json))
    golden = _j.loads(golden_json)
    return compare_batch_to_signature(current=current, golden_profile=golden)


def zone_message(zone: str) -> None:
    if zone == "Green":
        st.success("Green Zone: Process is healthy and eco-efficient.")
    elif zone == "Yellow":
        st.warning("Yellow Zone: Process is acceptable but should be improved.")
    else:
        st.error("Red Zone: High risk in energy/carbon/quality. Action needed.")


@st.cache_data
def monthly_energy_trend(features: pd.DataFrame) -> pd.DataFrame:
    df = features.sort_values("Batch_ID").reset_index(drop=True).copy()
    if df.empty:
        return pd.DataFrame(columns=["Month", "Energy_kWh", "Rolling_3M_kWh"])

    # Keep timeline readable even for very large datasets by adapting sampling span.
    target_hours = 36 * 30 * 24
    step_hours = max(1, target_hours // max(len(df), 1))
    df["Simulated_Date"] = pd.date_range(
        start=pd.Timestamp("2024-01-01"),
        periods=len(df),
        freq=f"{step_hours}h",
    )

    monthly = (
        df.set_index("Simulated_Date")["Total_Energy_kWh"]
        .resample("MS")
        .sum()
        .reset_index()
        .rename(columns={"Simulated_Date": "Month", "Total_Energy_kWh": "Energy_kWh"})
    )
    monthly["Rolling_3M_kWh"] = monthly["Energy_kWh"].rolling(window=3, min_periods=1).mean()
    if len(monthly) > 30:
        monthly = monthly.tail(30).reset_index(drop=True)
    return monthly


def to_pdf_bytes(lines: list[str]) -> bytes:
    # Minimal PDF generator to avoid external dependency.
    def esc(text: str) -> str:
        return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    y = 790
    content_lines = ["BT", "/F1 11 Tf", "50 790 Td"]
    first = True
    truncated = False
    for raw in lines:
        line = esc(raw)
        if first:
            content_lines.append(f"({line}) Tj")
            first = False
        else:
            content_lines.append("0 -16 Td")
            content_lines.append(f"({line}) Tj")
        y -= 16
        if y < 80:  # FIX P5: mark truncation instead of silently stopping
            truncated = True
            break
    if truncated:
        content_lines.append("0 -16 Td")
        content_lines.append("(... report truncated - export CSV for full data) Tj")
    content_lines.append("ET")
    stream = "\n".join(content_lines).encode("latin-1", errors="replace")

    objects = []
    objects.append(b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n")
    objects.append(b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n")
    objects.append(
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj\n"
    )
    objects.append(b"4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n")
    objects.append(f"5 0 obj << /Length {len(stream)} >> stream\n".encode("ascii") + stream + b"\nendstream endobj\n")

    pdf = b"%PDF-1.4\n"
    offsets = [0]
    for obj in objects:
        offsets.append(len(pdf))
        pdf += obj
    xref_pos = len(pdf)
    pdf += f"xref\n0 {len(offsets)}\n".encode("ascii")
    pdf += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        pdf += f"{off:010d} 00000 n \n".encode("ascii")
    pdf += f"trailer << /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF".encode("ascii")
    return pdf


inject_theme()

# ── Automatic responsive layout via CSS media queries ─────────────────────────
# No user action needed — the browser detects screen size automatically.
# Mobile styles kick in on any screen ≤768px (phones, small tablets).
st.markdown(
    """
    <style>
    /* ── MOBILE: auto-applied when screen width ≤ 768px ── */
    @media (max-width: 768px) {
        :root { --tap: 52px; }

        /* Larger readable text */
        body, html { font-size: 18px !important; }
        label,
        [data-testid="stText"],
        [data-testid="stMarkdownContainer"] p,
        [data-testid="stSidebar"] * { font-size: 17px !important; }

        /* Touch-friendly buttons */
        .stButton > button {
            min-height: var(--tap) !important;
            padding: 12px 18px !important;
            font-weight: 700 !important;
            border-radius: 12px !important;
            width: 100% !important;
        }

        /* Touch-friendly inputs and selects */
        button, input, select, textarea { font-size: 17px !important; }
        [data-testid="stSelectbox"] div[role="combobox"] { min-height: var(--tap) !important; }
        [data-testid="stNumberInput"] input { min-height: 44px !important; font-size: 17px !important; }
        [data-testid="stRadio"] label { padding: 10px 12px !important; border-radius: 10px !important; }

        /* Single-column metrics on mobile */
        [data-testid="metric-container"] { min-width: 100% !important; }

        /* Reduce chart padding */
        .block-container { padding-left: 0.5rem !important; padding-right: 0.5rem !important; }

        /* Metric values slightly smaller to fit */
        [data-testid="stMetricValue"] { font-size: 1.3rem !important; }
    }

    /* ── TABLET: 769px to 1024px ── */
    @media (min-width: 769px) and (max-width: 1024px) {
        .block-container { padding-left: 1rem !important; padding-right: 1rem !important; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Thread-safe background worker (FIXED: replaced mutable module-level globals
#    which caused duplicate threads on Streamlit reruns with st.cache_resource) ──
import queue as _queue

@st.cache_resource
def _get_worker_state() -> dict:
    """Singleton worker state — persists across Streamlit reruns safely."""
    return {
        "job_queue": _queue.Queue(),
        "last_status": None,
        "started": False,
        "lock": threading.Lock(),
    }

def _start_worker_once() -> None:
    state = _get_worker_state()
    with state["lock"]:
        if state["started"]:
            return
        def _worker():
            while True:
                try:
                    job = state["job_queue"].get(timeout=1)
                    try:
                        log_event("cache_rebuild_job_started", {"cache_dir": job["cache_dir"]})
                        from trackb_engine.bg_worker import run_cache_rebuild_job
                        info = run_cache_rebuild_job(
                            production_file=job["production_file"],
                            process_file=job["process_file"],
                            emission_factor=job["emission_factor"],
                            cache_dir=job["cache_dir"],
                        )
                        state["last_status"] = {"ok": True, "info": info}
                        log_event("cache_rebuild_job_completed", {"cache_dir": job["cache_dir"], "signature": info.get("signature")})
                    except Exception as e:
                        state["last_status"] = {"ok": False, "error": str(e)}
                        log_event("cache_rebuild_job_failed", {"error": str(e)})
                except _queue.Empty:
                    pass
        threading.Thread(target=_worker, daemon=True).start()
        state["started"] = True

def _enqueue_rebuild(production_file: str, process_file: str, emission_factor: float, cache_dir: str) -> None:
    state = _get_worker_state()
    state["job_queue"].put(
        {
            "production_file": production_file,
            "process_file": process_file,
            "emission_factor": emission_factor,
            "cache_dir": cache_dir,
        }
    )
    log_event("cache_rebuild_job_enqueued", {"cache_dir": cache_dir})

def _parse_auth_users(raw: str) -> dict[str, tuple[str, str]]:
    mapping: dict[str, tuple[str, str]] = {}
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        parts = token.split(":")
        if len(parts) == 3:
            user, pwd, role = parts
            mapping[user] = (pwd, role)
    return mapping


def _build_auth_credentials(raw: str) -> dict[str, dict[str, dict[str, str]]]:
    users = _parse_auth_users(raw)
    usernames: dict[str, dict[str, str]] = {}
    for username, (password, role) in users.items():
        usernames[username] = {
            "name": username,
            "email": f"{username}@local.invalid",
            "password": password,
            "role": role,
        }
    return {"usernames": usernames}


def _authenticate_user() -> tuple[str, str, object | None]:
    if not AUTH_ENABLED:
        return "guest", "Operator", None

    users = _parse_auth_users(AUTH_USERS)
    if not users:
        st.error("Authentication is enabled but no users are configured in AUTH_USERS.")
        st.stop()

    credentials = _build_auth_credentials(AUTH_USERS)
    authenticator = stauth.Authenticate(
        credentials=credentials,
        cookie_name=os.getenv("AUTH_COOKIE_NAME", "agpo_auth"),
        key=os.getenv("AUTH_COOKIE_KEY", "replace-this-cookie-key-in-env"),
        cookie_expiry_days=float(os.getenv("AUTH_COOKIE_EXPIRY_DAYS", "1")),
        auto_hash=True,
    )

    login_result = authenticator.login("Login", "main")
    if isinstance(login_result, tuple) and len(login_result) == 3:
        _name, authentication_status, username = login_result
    else:
        authentication_status = st.session_state.get("authentication_status")
        username = st.session_state.get("username")

    if authentication_status is False:
        st.error("Invalid username or password")
        st.stop()
    if authentication_status is None:
        st.info("Please log in to continue")
        st.stop()

    role = users.get(str(username), ("", "Operator"))[1]
    st.session_state["auth_user"] = str(username)
    st.session_state["auth_role"] = str(role)
    st.session_state["auth_status"] = True
    return str(username), str(role), authenticator


current_user, current_role, authenticator = _authenticate_user()

with st.sidebar:
    if AUTH_ENABLED and authenticator is not None:
        authenticator.logout("Logout", "sidebar")
    st.caption(f"Signed in as: {current_user} ({current_role})")
    # Language: English only
    st.header("Data Input")
    if st.button("Use Demo Data"):
        st.session_state["_force_demo_data"] = True
        st.session_state.pop("uploaded_production_meta", None)
        st.session_state.pop("uploaded_process_meta", None)
        st.session_state.pop("single_file_mode", None)
        st.session_state.pop("production_upload", None)
        st.session_state.pop("process_upload", None)
        st.success("Demo data loaded")
        st.rerun()
    force_demo = bool(st.session_state.pop("_force_demo_data", False))
    data_source = st.radio(
        "Choose data source",
        options=["Use built-in demo data", "Upload my factory files"],
        index=(0 if force_demo else 0),
        key="data_source",
    )
    uploaded_production_meta = None
    uploaded_process_meta = None
    if data_source == "Upload my factory files":
        if AUTH_ENABLED and current_role not in {"Manager", "Executive"}:
            st.error("Upload requires Manager or Executive role.")
            st.stop()
        if bool(st.session_state.get("uploads_blocked_session", False)):
            st.error(tr("Uploads blocked"))
            st.stop()
        st.caption("Step 1: Upload Production file, then Step 2: Upload Process file.")
        limit_mb = int(int(MAX_UPLOAD_BYTES) // (1024 * 1024))
        st.caption(f"Limit {limit_mb}MB per file • XLSX or CSV")
        uploads_dir = Path(__file__).parent / "artifacts" / "uploads"
        uploads_dir.mkdir(parents=True, exist_ok=True)

        uploaded_production_meta = st.session_state.get("uploaded_production_meta")
        uploaded_process_meta = st.session_state.get("uploaded_process_meta")

        if not uploaded_production_meta:
            production_upload = st.file_uploader(
                "Step 1 - Production file (.xlsx or .csv)",
                type=["xlsx", "csv"],
                key="production_upload",
                help=f"Max {limit_mb}MB; allowed types: .xlsx, .csv",
            )
            if production_upload is not None:
                now = pd.Timestamp.utcnow().timestamp()
                attempts = st.session_state.get("upload_attempts", [])
                block_until = float(st.session_state.get("upload_block_until", 0.0))
                attempts = [t for t in attempts if now - t < float(UPLOAD_RATE_WINDOW_SEC)]
                if now < block_until:
                    st.error("Uploads temporarily blocked due to rate limit. Try later.")
                    st.stop()
                if len(attempts) >= int(UPLOAD_RATE_LIMIT):
                    backoff = min(float(BACKOFF_MAX_SECONDS), 2 ** len(attempts))
                    st.session_state["upload_block_until"] = now + backoff
                    st.error("Upload rate exceeded. Backing off.")
                    st.stop()
                prod_path, prod_stamp = persist_uploaded_file(production_upload, uploads_dir, "production_uploaded")
                attempts.append(now)
                st.session_state["upload_attempts"] = attempts
                st.session_state["uploaded_production_meta"] = {
                    "name": production_upload.name,
                    "path": str(prod_path),
                    "stamp": [prod_stamp[0], int(prod_stamp[1])],
                }
                st.rerun()
        else:
            st.success(f"Production file selected: {uploaded_production_meta['name']}")
            if st.button("Change Production file", key="change_production_file"):
                st.session_state.pop("uploaded_production_meta", None)
                st.session_state.pop("uploaded_process_meta", None)
                st.session_state.pop("single_file_mode", None)
                st.session_state.pop("production_upload", None)
                st.session_state.pop("process_upload", None)
                st.rerun()

        uploaded_production_meta = st.session_state.get("uploaded_production_meta")
        if uploaded_production_meta:
            if "single_file_mode" not in st.session_state:
                try:
                    auto_sf = _detect_single_file_candidate(uploaded_production_meta["path"])
                    st.session_state["single_file_mode"] = bool(auto_sf)
                except Exception:
                    pass
            single_file_mode = st.toggle(
                "Single-file mode (use production file for process as well)",
                value=bool(st.session_state.get("single_file_mode", False)),
                key="single_file_mode",
            )
            if single_file_mode:
                mirrored = {
                    "name": f"{uploaded_production_meta['name']} (same as production)",
                    "path": uploaded_production_meta["path"],
                    "stamp": uploaded_production_meta["stamp"],
                }
                st.session_state["uploaded_process_meta"] = mirrored
                uploaded_process_meta = mirrored
                st.success(f"Process file selected: {uploaded_process_meta['name']}")
            else:
                current_process = st.session_state.get("uploaded_process_meta")
                if current_process and current_process.get("path") == uploaded_production_meta["path"] and "same as production" in str(
                    current_process.get("name", "")
                ):
                    st.session_state.pop("uploaded_process_meta", None)
                    current_process = None
                uploaded_process_meta = current_process
                if not uploaded_process_meta:
                    process_upload = st.file_uploader(
                        "Step 2 - Process file (.xlsx or .csv)",
                        type=["xlsx", "csv"],
                        key="process_upload",
                        help=f"Max {limit_mb}MB; allowed types: .xlsx, .csv",
                    )
                    if process_upload is not None:
                        now = pd.Timestamp.utcnow().timestamp()
                        attempts = st.session_state.get("upload_attempts", [])
                        block_until = float(st.session_state.get("upload_block_until", 0.0))
                        attempts = [t for t in attempts if now - t < float(UPLOAD_RATE_WINDOW_SEC)]
                        if now < block_until:
                            st.error("Uploads temporarily blocked due to rate limit. Try later.")
                            st.stop()
                        if len(attempts) >= int(UPLOAD_RATE_LIMIT):
                            backoff = min(float(BACKOFF_MAX_SECONDS), 2 ** len(attempts))
                            st.session_state["upload_block_until"] = now + backoff
                            st.error("Upload rate exceeded. Backing off.")
                            st.stop()
                        proc_path, proc_stamp = persist_uploaded_file(process_upload, uploads_dir, "process_uploaded")
                        attempts.append(now)
                        st.session_state["upload_attempts"] = attempts
                        st.session_state["uploaded_process_meta"] = {
                            "name": process_upload.name,
                            "path": str(proc_path),
                            "stamp": [proc_stamp[0], int(proc_stamp[1])],
                        }
                        st.rerun()
                else:
                    st.success(f"Process file selected: {uploaded_process_meta['name']}")
                    if st.button("Change Process file", key="change_process_file"):
                        st.session_state.pop("uploaded_process_meta", None)
                        st.session_state.pop("process_upload", None)
                        st.rerun()
        else:
            st.info("Upload Production file first to continue.")
    else:
        st.caption("Using demo dataset.")

    st.header("Factory Maturity Mode")
    mode = st.selectbox(
        "Select factory level",
        options=[
            "Level 1 - Manual",
            "Level 2 - Semi-Digital",
            "Level 3 - Enterprise",
        ],
        index=0,
    )

    st.header("Dashboard Experience")
    ui_mode = st.radio(
        "Complexity",
        options=["Simple", "Advanced"],
        index=0,
        horizontal=True,
    )
    is_simple = ui_mode == "Simple"
    if mode == "Level 3 - Enterprise" and not is_simple:
        role = st.selectbox("Role", options=["Operator", "Manager", "Executive"], index=0)
    else:
        role = "Operator"
    st.session_state["fast_start"] = True

    st.header("Business Goal")
    goal = st.selectbox("Goal", options=list(DEFAULT_SCENARIOS.keys()), index=0)
    if not is_simple:
        strictness = st.select_slider("Strictness", options=["Open", "Standard", "Strict"], value="Standard")
        with st.expander("Advanced Inputs", expanded=False):
            emission_factor = st.slider("Emission factor (kg CO2/kWh)", 0.30, 1.20, float(DEFAULT_EMISSION_FACTOR), 0.01)
            energy_cost = st.slider("Energy cost (USD/kWh)", 0.05, 0.40, float(DEFAULT_ENERGY_COST), 0.01)
            annual_batches = st.number_input("Annual batches", min_value=100, max_value=100000, value=int(DEFAULT_ANNUAL_BATCHES), step=100)
            use_feature_store = st.toggle("Use cache", value=True)
            force_rebuild_click = st.button("Refresh cache")
            if force_rebuild_click:
                st.session_state["rebuild_requested"] = True
    else:
        strictness = "Standard"
        emission_factor = float(DEFAULT_EMISSION_FACTOR)
        energy_cost = float(DEFAULT_ENERGY_COST)
        annual_batches = int(DEFAULT_ANNUAL_BATCHES)
        use_feature_store = True
        force_rebuild_click = False
        st.caption("Simple mode uses recommended defaults for faster experience.")

    # ── FEATURE 5: Industry & Regulatory Target Selector ──────────────────
    # Satisfies: "Integrate regulatory requirements and sustainability
    # commitments into dynamic goal-setting mechanisms" (Universal Objective 1)
    st.header("🏭 Industry & Regulatory Targets")
    _INDUSTRY_PROFILES = {
        "Pharmaceutical": {"carbon_target_kg": 2.5,  "energy_target_kwh": 180.0, "standard": "ISO 14001 / GHG Protocol"},
        "Automotive":     {"carbon_target_kg": 3.8,  "energy_target_kwh": 320.0, "standard": "IATF 16949 / CDP"},
        "Food & Beverage":{"carbon_target_kg": 1.8,  "energy_target_kwh": 140.0, "standard": "ISO 50001 / SBTi"},
        "Textiles":       {"carbon_target_kg": 4.2,  "energy_target_kwh": 260.0, "standard": "ZDHC / Higg Index"},
        "Electronics":    {"carbon_target_kg": 1.2,  "energy_target_kwh": 95.0,  "standard": "IPC-1401 / RoHS"},
        "Custom":         {"carbon_target_kg": None,  "energy_target_kwh": None,  "standard": "User-defined"},
    }
    selected_industry = st.selectbox(
        "Select industry",
        options=list(_INDUSTRY_PROFILES.keys()),
        index=0,
        key="industry_selector",
        help="Sets regulatory carbon & energy targets for your sector",
    )
    _iprof = _INDUSTRY_PROFILES[selected_industry]
    if selected_industry == "Custom" and not is_simple:
        reg_carbon_target  = st.number_input("Regulatory carbon target (kg CO₂/batch)",  min_value=0.1, max_value=100.0, value=2.5, step=0.1)
        reg_energy_target  = st.number_input("Regulatory energy target (kWh/batch)",      min_value=1.0, max_value=1000.0, value=180.0, step=5.0)
        reg_standard_label = "Custom"
    else:
        reg_carbon_target  = float(_iprof["carbon_target_kg"] or 2.5)
        reg_energy_target  = float(_iprof["energy_target_kwh"] or 180.0)
        reg_standard_label = str(_iprof["standard"])
    st.caption(f"Standard: **{reg_standard_label}** | Carbon ≤ {reg_carbon_target} kg | Energy ≤ {reg_energy_target} kWh")
    # Store for use across the page
    st.session_state["reg_carbon_target"] = reg_carbon_target
    st.session_state["reg_energy_target"] = reg_energy_target
    st.session_state["selected_industry"] = selected_industry
    st.session_state["reg_standard_label"] = reg_standard_label

demo_production_path = Path(PRODUCTION_DATA_FILE)
demo_process_path = Path(PROCESS_DATA_FILE)
demo_available = demo_production_path.exists() and demo_process_path.exists()
if not bool(_DISABLE_BG_WORKER):
    _start_worker_once()

if data_source == "Upload my factory files" and uploaded_production_meta and uploaded_process_meta:
    production_path = Path(str(uploaded_production_meta["path"]))
    process_path = Path(str(uploaded_process_meta["path"]))
    if not production_path.exists() or not process_path.exists():
        st.error("Uploaded file reference was lost. Please upload again.")
        st.session_state.pop("uploaded_production_meta", None)
        st.session_state.pop("uploaded_process_meta", None)
        st.session_state.pop("production_upload", None)
        st.session_state.pop("process_upload", None)
        st.stop()
    prod_stamp = (str(uploaded_production_meta["stamp"][0]), int(uploaded_production_meta["stamp"][1]))
    proc_stamp = (str(uploaded_process_meta["stamp"][0]), int(uploaded_process_meta["stamp"][1]))
    cache_dir = FEATURE_STORE_UPLOADS_DIR
    data_source_label = "Uploaded files"
elif data_source == "Upload my factory files":
    if demo_available:
        st.info("Upload both files to use your data. Showing demo data for now.")
        production_path = demo_production_path
        process_path = demo_process_path
        prod_file_stamp = _stamp(str(production_path))
        proc_file_stamp = _stamp(str(process_path))
        prod_stamp = (str(prod_file_stamp[0]), int(prod_file_stamp[1]))
        proc_stamp = (str(proc_file_stamp[0]), int(proc_file_stamp[1]))
        cache_dir = FEATURE_STORE_DIR
        data_source_label = "Built-in demo data (waiting for upload)"
    else:
        st.warning("Please upload your dataset files to continue")
        st.info("Required files: _h_batch_production_data.xlsx and _h_batch_process_data_copy.xlsx")
        st.stop()
else:
    if not demo_available:
        st.warning("Please upload your dataset files to continue")
        st.info("Required files: _h_batch_production_data.xlsx and _h_batch_process_data_copy.xlsx")
        st.stop()
    production_path = demo_production_path
    process_path = demo_process_path
    prod_file_stamp = _stamp(str(production_path))
    proc_file_stamp = _stamp(str(process_path))
    prod_stamp = (str(prod_file_stamp[0]), int(prod_file_stamp[1]))
    proc_stamp = (str(proc_file_stamp[0]), int(proc_file_stamp[1]))
    cache_dir = FEATURE_STORE_DIR
    data_source_label = "Built-in demo data"

try:
    if bool(st.session_state.get("rebuild_requested", False)):
        _enqueue_rebuild(str(production_path), str(process_path), float(emission_factor), cache_dir)
        st.session_state["rebuild_requested"] = False
        st.info("Cache rebuild scheduled in background.")
    artifacts, cache_info = load_pipeline(
        production_file=str(production_path),
        process_file=str(process_path),
        cache_dir=cache_dir,
        emission_factor=emission_factor,
        use_feature_store=use_feature_store,
        force_rebuild=False,
        prod_stamp=prod_stamp,
        proc_stamp=proc_stamp,
    )
except Exception as exc:
    if data_source == "Upload my factory files" and demo_available:
        log_event("upload_processing_failed", {"user": current_user, "reason": str(exc)})
        st.error(f"Uploaded files could not be processed: {exc}")
        st.warning("Showing demo data instead. Upload corrected files to switch.")
        production_path = demo_production_path
        process_path = demo_process_path
        prod_file_stamp = _stamp(str(production_path))
        proc_file_stamp = _stamp(str(process_path))
        artifacts, cache_info = load_pipeline(
            production_file=str(production_path),
            process_file=str(process_path),
            cache_dir=FEATURE_STORE_DIR,  # FIX B4: corrected indentation
            emission_factor=emission_factor,
            use_feature_store=use_feature_store,
            force_rebuild=False,
            prod_stamp=(str(prod_file_stamp[0]), int(prod_file_stamp[1])),
            proc_stamp=(str(proc_file_stamp[0]), int(proc_file_stamp[1])),
        )
        data_source_label = "Built-in demo data (fallback)"
    else:
        log_event("pipeline_failed", {"reason": str(exc)})
        st.error(f"Data pipeline failed: {exc}")
        st.stop()
features = artifacts.features.copy()
optimizer = MultiObjectiveOptimizer(features)
targets = build_targets(features, strictness)
pipeline_mode = str(artifacts.cleaning_report.get("data_mode", "Full"))
batch_strategy = str(artifacts.cleaning_report.get("batch_strategy", "provided"))

summary = _data_check_summary(artifacts.production_raw, artifacts.process_timeseries_raw)
st.subheader("Data Check")
dc1, dc2, dc3 = st.columns(3)
dc1.metric("Rows (total)", f"{summary['rows']}")
dc2.metric("Missing %", f"{summary['missing_pct']:.2f}%")
dc3.metric("Out-of-range %", f"{summary['out_of_range_pct']:.2f}%")

weights = DEFAULT_SCENARIOS[goal]
if mode == "Level 3 - Enterprise" and role in {"Manager", "Executive"}:
    with st.sidebar.expander("Advanced: objective weights"):
        custom = {}
        for obj in ["Yield_Percent", "Quality_Score", "Performance_Score", "Total_Energy_kWh", "Carbon_kg", "Eco_Efficiency_Score"]:
            default_val = float(weights.get(obj, 0.0))
            custom[obj] = st.slider(obj.replace("_", " "), 0.0, 1.0, default_val, 0.01)
        total = sum(custom.values())
        if total > 0:
            weights = {k: v / total for k, v in custom.items()}

import json as _json_mod
_weights_json = _json_mod.dumps(weights, sort_keys=True)
if bool(st.session_state.get("fast_start", False)):
    ranked = pd.DataFrame()
    recommended = features.iloc[0]
else:
    ranked = _cached_rank_batches(features, _weights_json, strictness)
    recommended = ranked.iloc[0]
pareto = pd.DataFrame()

manager = GoldenSignatureManager(GOLDEN_SIGNATURE_FILE)
payload = manager.load()
if not payload and not bool(st.session_state.get("fast_start", False)):
    with st.spinner("⏳ Generating golden signatures for first time... (takes ~5 sec, cached after)"):
        payload = manager.generate_signatures(
            optimizer=optimizer,
            scenarios=DEFAULT_SCENARIOS,
            targets=targets,
            top_n=3,
        )

# ── AUTO Golden Signature Update ─────────────────────────────────────────────
# Each time a dataset is loaded, silently compare the top-ranked batch against
# the saved golden signature and auto-promote if it scores higher.
if not ranked.empty:
    _auto_candidate = ranked.iloc[0]
    _auto_score = float(_auto_candidate.get("Scenario_Score", 0.0))
    _promoted, payload = manager.promote_if_better(
        payload=payload,
        scenario_name=goal,
        candidate_profile=_auto_candidate,
        candidate_score=_auto_score,
        source_tag="auto_update",
    )
    if _promoted:
        log_event("golden_auto_promoted", {
            "scenario": goal,
            "batch": str(_auto_candidate.get("Batch_ID", "")),
            "score": str(_auto_score),
        })

# ── Golden Signature Info Panel (read-only) ───────────────────────────────────
_current_sig = payload.get("signatures", {}).get(goal, {})
with st.expander("🏅 Current Golden Signature", expanded=False):
    if _current_sig:
        gs1, gs2, gs3 = st.columns(3)
        gs1.metric("Best Batch", str(_current_sig.get("batch_id", "N/A")))
        gs2.metric("Best Score", f"{float(_current_sig.get('score', 0.0)):.4f}")
        _gs_ts = _current_sig.get("promoted_at_utc") or payload.get("generated_at_utc", "—")
        gs3.metric("Last Updated", str(_gs_ts)[:19].replace("T", " "))
        _gs_profile = _current_sig.get("profile", {})
        if _gs_profile:
            st.caption("Key parameters of the best-performing batch:")
            _display_keys = ["Yield_Percent", "Quality_Score", "Total_Energy_kWh",
                             "Carbon_kg", "Eco_Efficiency_Score"]
            _gs_df = pd.DataFrame([{k: _gs_profile.get(k, "—") for k in _display_keys}])
            st.dataframe(_gs_df, use_container_width=True)
    else:
        st.info("No golden signature recorded yet. Upload a dataset to generate one.")

# ── Signature History Log ─────────────────────────────────────────────────────
history_path = Path("artifacts/golden_signature_history.csv")
history_path.parent.mkdir(parents=True, exist_ok=True)
_sig_ts = payload.get("updated_at_utc") or payload.get("generated_at_utc") or pd.Timestamp.utcnow().isoformat()
history_rows = []
for scenario_name, data in payload.get("signatures", {}).items():
    history_rows.append({
        "timestamp": _sig_ts,
        "scenario": scenario_name,
        "score": float(data.get("score", 0.0)),
        "batch_id": str(data.get("batch_id", "")),
        "promoted_from": str(data.get("promoted_from", "initial")),
        "approved_by": str(data.get("source_tag", "system")),
    })
if history_rows:
    hist_new = pd.DataFrame(history_rows)
    if history_path.exists():
        try:
            hist_old = pd.read_csv(history_path)
        except Exception as exc:
            logger.exception("Failed to read history file: %s", history_path)
            st.warning("Unable to read existing history log. Rebuilding history file.")
            hist_old = pd.DataFrame()
        merged = pd.concat([hist_old, hist_new], ignore_index=True).drop_duplicates(
            subset=["timestamp", "scenario"], keep="last"
        )
    else:
        merged = hist_new
    merged.to_csv(history_path, index=False)

selected_signature = payload.get("signatures", {}).get(goal, {})
if selected_signature.get("profile"):
    golden_profile = selected_signature["profile"]
else:
    golden_profile = recommended.to_dict()

st.title("Smart Pollution Monitoring System Dashboard")
st.caption("Simple by default. Advanced insights appear only when factory level increases.")
st.caption(f"Data Source: {data_source_label} | Factory Level: {mode} | Goal: {goal}")
st.markdown(
    "Real-time **pollution monitoring system** for **air quality monitoring**, "
    "**noise pollution monitoring**, **water pollution tracking**, and "
    "**plastic waste monitoring** with hotspot detection and trend analytics."
)
if not is_simple:
    st.caption(f"Pipeline Mode: {pipeline_mode} | Batch Strategy: {batch_strategy}")
    st.caption(f"Experience Mode: {ui_mode}")
if pipeline_mode == "Minimal":
    st.warning("Minimal mode active: limited data available, showing monitoring-oriented optimization.")
elif pipeline_mode == "Partial":
    st.info("Partial mode active: system generated robust fallback features from available signals.")
st.info("Flow: Top = Key KPI | Middle = Trend + Alerts | Bottom = Action recommendations")

if mode == "Level 3 - Enterprise":
    with st.expander("Level 3 quick guide (simple words)", expanded=is_simple):
        st.markdown(
            "1. Select a current batch.\n"
            "2. Check three cards: Energy, Quality, Eco Score.\n"
            "3. Read the recommendation box first.\n"
            "4. Use comparison and Pareto only for deeper decisions.\n"
            "5. Use export buttons to share manager/executive reports."
        )

# FIX P3: cache Batch_ID list to avoid re-building on every Streamlit rerun
_batch_cache_key = (prod_stamp, proc_stamp)
if st.session_state.get("_batch_id_cache_key") != _batch_cache_key:
    st.session_state["_batch_id_list"] = features["Batch_ID"].tolist()
    st.session_state["_batch_id_cache_key"] = _batch_cache_key
_batch_id_list = st.session_state["_batch_id_list"]

current_batch_id = st.selectbox("Current batch", options=_batch_id_list, index=0)
current_row = features.loc[features["Batch_ID"] == current_batch_id].iloc[0].copy()

def _qs(row: pd.Series, feats: pd.DataFrame, w: dict[str, float]) -> float:
    def _npos(k: str) -> float:
        s = pd.to_numeric(feats[k], errors="coerce")
        mx = float(s.max())
        mn = float(s.min())
        v = float(pd.to_numeric(row.get(k, 0.0), errors="coerce"))
        if mx == mn:
            return 0.0
        return max(0.0, min(1.0, (v - mn) / (mx - mn)))
    def _nneg(k: str) -> float:
        s = pd.to_numeric(feats[k], errors="coerce")
        mx = float(s.max())
        mn = float(s.min())
        v = float(pd.to_numeric(row.get(k, 0.0), errors="coerce"))
        if mx == mn:
            return 0.0
        return max(0.0, min(1.0, 1.0 - (v - mn) / (mx - mn)))
    sc = 0.0
    sc += float(w.get("Yield_Percent", 0.0)) * _npos("Yield_Percent")
    sc += float(w.get("Quality_Score", 0.0)) * _npos("Quality_Score")
    sc += float(w.get("Eco_Efficiency_Score", 0.0)) * _npos("Eco_Efficiency_Score")
    if "Performance_Score" in feats.columns:
        sc += float(w.get("Performance_Score", 0.0)) * _npos("Performance_Score")
    sc += float(w.get("Total_Energy_kWh", 0.0)) * _nneg("Total_Energy_kWh")
    sc += float(w.get("Carbon_kg", 0.0)) * _nneg("Carbon_kg")
    tw = sum(float(v) for v in w.values()) or 1.0
    return max(0.0, min(1.0, sc / tw))
try:
    _score_row = ranked.loc[ranked["Batch_ID"] == current_batch_id]
    if not _score_row.empty and "Scenario_Score" in _score_row.columns:
        _current_score = float(_score_row.iloc[0]["Scenario_Score"])
    else:
        _current_score = _qs(current_row, features, weights)
except Exception:
    _current_score = _qs(current_row, features, weights)
_pct = max(0.0, min(1.0, _current_score))

# Top Row
t1, t2, t3 = st.columns(3)
t1.metric("Energy used this batch (kWh)", f'{float(current_row["Total_Energy_kWh"]):.2f}')
t2.metric("Product quality score", f'{float(current_row["Quality_Score"]):.2f}')
t3.metric("Overall eco score", f'{float(current_row["Eco_Efficiency_Score"]):.2f}')
st.caption("Higher quality/eco is better. Lower energy is better.")
with st.expander("ROI Calculator", expanded=False):
    cur_energy = float(current_row["Total_Energy_kWh"])
    gold_energy = float(golden_profile.get("Total_Energy_kWh", cur_energy))
    roi = estimate_roi(
        current_energy_kwh=cur_energy,
        golden_energy_kwh=gold_energy,
        energy_cost_per_kwh=float(energy_cost),
        annual_batches=int(annual_batches),
    )
    c1, c2, c3 = st.columns(3)
    c1.metric("Energy delta (kWh)", f'{roi["delta_energy_kwh"]:.3f}')
    c2.metric("Savings per batch (USD)", f'{roi["savings_per_batch_usd"]:.2f}')
    c3.metric("Annual savings (USD)", f'{roi["annual_savings_usd"]:.2f}')

# Red Zone alerting
def _send_alert(subject: str, body: str) -> bool:
    if not ALERT_ENABLED or not ALERT_EMAIL_TO or not ALERT_SMTP_SERVER or not ALERT_SMTP_USER or not ALERT_SMTP_PASS:
        log_event("red_zone_alert_skipped", {"reason": "not_configured"})
        return False
    try:
        msg = f"From: {ALERT_SMTP_USER}\r\nTo: {ALERT_EMAIL_TO}\r\nSubject: {subject}\r\n\r\n{body}"
        with smtplib.SMTP(ALERT_SMTP_SERVER, ALERT_SMTP_PORT, timeout=10) as s:
            s.starttls()
            s.login(ALERT_SMTP_USER, ALERT_SMTP_PASS)
            s.sendmail(ALERT_SMTP_USER, [ALERT_EMAIL_TO], msg.encode("utf-8"))
        log_event("red_zone_alert_sent", {"to": ALERT_EMAIL_TO})
        return True
    except Exception as e:
        log_event("red_zone_alert_failed", {"error": str(e)})
        return False

if "_alerted_batch_ids" not in st.session_state:
    st.session_state["_alerted_batch_ids"] = set()
zone = str(current_row.get("Green_Zone", "Red"))
auto_alert = st.toggle("Auto alert on Red Zone", value=False, key="auto_alert_red_zone")
if zone == "Red":
    if auto_alert and current_batch_id not in st.session_state["_alerted_batch_ids"]:
        ok = _send_alert(
            subject=f"AGPO Alert: Red Zone batch {current_batch_id}",
            body=f"Batch {current_batch_id} is in Red Zone.\nEnergy={current_row['Total_Energy_kWh']:.2f} kWh, Quality={current_row['Quality_Score']:.2f}, Eco={current_row['Eco_Efficiency_Score']:.2f}",
        )
        if ok:
            st.session_state["_alerted_batch_ids"].add(current_batch_id)
    if st.button("Send alert now", type="secondary"):
        ok = _send_alert(
            subject=f"AGPO Alert: Red Zone batch {current_batch_id}",
            body=f"Batch {current_batch_id} is in Red Zone.\nEnergy={current_row['Total_Energy_kWh']:.2f} kWh, Quality={current_row['Quality_Score']:.2f}, Eco={current_row['Eco_Efficiency_Score']:.2f}",
        )
        st.success("Alert sent") if ok else st.warning("Alert skipped (not configured)")

zone_message(str(current_row.get("Green_Zone", "Red")))

# Middle Row: visible to all levels
if mode == "Level 1 - Manual":
    st.subheader(tr("Quick Panel"))
    voice_on = st.toggle(tr("Voice guidance"), value=False, key="voice_guidance_toggle")
    st.caption(tr("Using sidebar to upload"))
    primary_click = st.button(f"🛡️ {tr('Check Safety')}", use_container_width=True)
    if primary_click:
        is_green = str(current_row.get("Green_Zone", "Red")) == "Green"
        if is_green:
            st.success(tr("SAFE"))
            if voice_on:
                speak(tr("SAFE"))
        else:
            st.error(tr("RISK DETECTED"))
            if voice_on:
                speak(tr("RISK DETECTED"))

    # Skip heavy charts for Level 1; continue with simple guidance
    st.stop()
st.subheader("Energy trend over time")
st.caption("Bars = monthly energy used. Line = 3-month average.")
monthly = monthly_energy_trend(features)
_limit = None
if str(st.session_state.get("device_mode", "Auto")) == "Mobile":
    _limit = 6
elif is_simple:
    _limit = 12
if _limit and len(monthly) > _limit:
    monthly = monthly.tail(_limit).copy()
energy_bars = (
    alt.Chart(monthly)
    .mark_bar(
        color="#1ebd83",
        opacity=0.55,
    )
    .encode(
        x=alt.X(
            "Month:T",
            title="Month",
            axis=alt.Axis(format="%b %Y", labelAngle=-25, grid=False, tickCount=8, labelColor="#ffe7c2"),
        ),
        y=alt.Y("Energy_kWh:Q", title="Total Energy (kWh)"),
        tooltip=[
            alt.Tooltip("Month:T", title="Month", format="%b %Y"),
            alt.Tooltip("Energy_kWh:Q", title="Energy (kWh)", format=".2f"),
            alt.Tooltip("Rolling_3M_kWh:Q", title="3M Avg (kWh)", format=".2f"),
        ],
    )
)
trend_line = (
    alt.Chart(monthly)
    .mark_line(color="#9bd9ff", strokeWidth=3)
    .encode(
        x=alt.X("Month:T", title="Month"),
        y=alt.Y("Rolling_3M_kWh:Q", title="Total Energy (kWh)"),
    )
)
monthly_chart = (
    alt.layer(energy_bars, trend_line)
    .resolve_scale(y="shared")
    .properties(height=320)
    .configure_view(stroke=None)
    .configure_axis(
        labelFontSize=12,
        titleFontSize=13,
        titleColor="#eaf4ff",
        labelColor="#d8e8f6",
        gridColor="#254a71",
        domainColor="#2d5a83",
        tickColor="#2d5a83",
    )
)
st.altair_chart(monthly_chart, use_container_width=True)

# Level 2 and Level 3 blocks
show_detail_analytics = not is_simple
if mode in {"Level 2 - Semi-Digital", "Level 3 - Enterprise"} and is_simple:
    show_detail_analytics = st.toggle("Show detailed analytics", value=False, key="show_detail_analytics")

if mode in {"Level 2 - Semi-Digital", "Level 3 - Enterprise"} and show_detail_analytics and not bool(st.session_state.get("fast_start", False)):
    st.subheader("Detailed process view (current batch)")
    st.caption("Use this to diagnose root cause when performance drops.")
    proc = artifacts.process_timeseries_raw.copy()
    proc = proc.loc[proc["Batch_ID"] == current_batch_id].copy()
    for col in ["Time_Minutes", "Temperature_C", "Motor_Speed_RPM"]:
        proc[col] = pd.to_numeric(proc[col], errors="coerce")
    proc = proc.dropna(subset=["Time_Minutes"])
    _max_points = 900
    if str(st.session_state.get("device_mode", "Auto")) == "Mobile":
        _max_points = 400
    if len(proc) > _max_points:
        step = max(1, len(proc) // _max_points)
        proc = proc.iloc[::step].copy()

    win = max(5, len(proc) // 50)
    proc["Temp_Roll"] = pd.to_numeric(proc["Temperature_C"], errors="coerce").rolling(window=win, min_periods=1).mean()
    proc["RPM_Roll"] = pd.to_numeric(proc["Motor_Speed_RPM"], errors="coerce").rolling(window=win, min_periods=1).mean()
    brush = alt.selection_interval(encodings=["x"])

    p1, p2 = st.columns(2)
    with p1:
        temp_area = (
            alt.Chart(proc)
            .mark_area(color="#f59e0b", opacity=0.18)
            .encode(
                x=alt.X("Time_Minutes:Q", title="Time (min)"),
                y=alt.Y("Temperature_C:Q", title="Temperature (C)"),
                tooltip=[alt.Tooltip("Time_Minutes:Q", title="Time"), alt.Tooltip("Temperature_C:Q", title="Temp (C)", format=".2f")],
            )
        )
        temp_line = (
            alt.Chart(proc)
            .mark_line(color="#22c98a", strokeWidth=3)
            .encode(
                x=alt.X("Time_Minutes:Q", title="Time (min)"),
                y=alt.Y("Temp_Roll:Q", title="Temperature (C)"),
                tooltip=[alt.Tooltip("Time_Minutes:Q", title="Time"), alt.Tooltip("Temp_Roll:Q", title="Avg Temp (C)", format=".2f")],
            )
        )
        temp_chart = (
            alt.layer(temp_area, temp_line)
            .resolve_scale(y="shared")
            .properties(height=280)
            .configure_view(stroke=None)
            .configure_axis(
                labelFontSize=12,
                titleFontSize=13,
                titleColor="#eaf4ff",
                labelColor="#d8e8f6",
                gridColor="#254a71",
                domainColor="#2d5a83",
                tickColor="#2d5a83",
            )
            .add_params(brush)
        )
        st.altair_chart(temp_chart, use_container_width=True)
    with p2:
        rpm_area = (
            alt.Chart(proc)
            .mark_area(color="#9bd9ff", opacity=0.18)
            .encode(
                x=alt.X("Time_Minutes:Q", title="Time (min)"),
                y=alt.Y("Motor_Speed_RPM:Q", title="RPM"),
                tooltip=[alt.Tooltip("Time_Minutes:Q", title="Time"), alt.Tooltip("Motor_Speed_RPM:Q", title="RPM", format=".2f")],
            )
            .transform_filter(brush)
        )
        rpm_line = (
            alt.Chart(proc)
            .mark_line(color="#9bd9ff", strokeWidth=3)
            .encode(
                x=alt.X("Time_Minutes:Q", title="Time (min)"),
                y=alt.Y("RPM_Roll:Q", title="RPM"),
                tooltip=[alt.Tooltip("Time_Minutes:Q", title="Time"), alt.Tooltip("RPM_Roll:Q", title="Avg RPM", format=".2f")],
            )
            .transform_filter(brush)
        )
        rpm_chart = (
            alt.layer(rpm_area, rpm_line)
            .resolve_scale(y="shared")
            .properties(height=280)
            .configure_view(stroke=None)
            .configure_axis(
                labelFontSize=12,
                titleFontSize=13,
                titleColor="#eaf4ff",
                labelColor="#d8e8f6",
                gridColor="#254a71",
                domainColor="#2d5a83",
                tickColor="#2d5a83",
            )
        )
        st.altair_chart(rpm_chart, use_container_width=True)

    comp = compare_batch_to_signature(current=current_row, golden_profile=golden_profile)
    focus = comp[comp["Metric"].isin(["Yield_Percent", "Quality_Score", "Total_Energy_kWh", "Carbon_kg", "Eco_Efficiency_Score"])]
    st.subheader(tr("Comparison View"))
    st.dataframe(comp, use_container_width=True)

    st.subheader("Are we better or worse than the best batch?")
    st.caption("Negative energy difference is good. Positive quality/eco difference is good.")
    d1, d2, d3 = st.columns(3)
    energy_dev = focus.loc[focus["Metric"] == "Total_Energy_kWh"]
    quality_dev = focus.loc[focus["Metric"] == "Quality_Score"]
    eco_dev = focus.loc[focus["Metric"] == "Eco_Efficiency_Score"]
    if not energy_dev.empty:
        d1.metric("Energy difference vs best (%)", f'{float(energy_dev.iloc[0]["Deviation_%"]):+.2f}%')
    if not quality_dev.empty:
        d2.metric("Quality difference vs best (%)", f'{float(quality_dev.iloc[0]["Deviation_%"]):+.2f}%')
    if not eco_dev.empty:
        d3.metric("Eco difference vs best (%)", f'{float(eco_dev.iloc[0]["Deviation_%"]):+.2f}%')

    st.subheader("Current batch vs best benchmark")
    st.caption("Green bar = current batch. Blue bar = best benchmark (golden signature).")
    bench = pd.DataFrame(
        [
            ["Quality Score", float(current_row["Quality_Score"]), float(golden_profile.get("Quality_Score", current_row["Quality_Score"]))],
            ["Energy (kWh)", float(current_row["Total_Energy_kWh"]), float(golden_profile.get("Total_Energy_kWh", current_row["Total_Energy_kWh"]))],
            ["Eco Score", float(current_row["Eco_Efficiency_Score"]), float(golden_profile.get("Eco_Efficiency_Score", current_row["Eco_Efficiency_Score"]))],
        ],
        columns=["Metric", "Current", "Golden"],
    )
    bench_long = bench.melt(id_vars="Metric", var_name="Type", value_name="Value")
    bench_chart = (
        alt.Chart(bench_long)
        .mark_bar()
        .encode(
            x="Metric:N",
            y="Value:Q",
            color=alt.Color("Type:N", scale=alt.Scale(range=["#22c98a", "#9bd9ff"])),
            xOffset="Type:N",
            tooltip=["Metric", "Type", alt.Tooltip("Value:Q", format=".2f")],
        )
        .properties(height=290)
        .configure_view(stroke=None)
    )
    st.altair_chart(bench_chart, use_container_width=True)

    st.subheader("Estimated carbon impact")
    st.caption("Shows this batch CO2 and potential reduction if we move to best benchmark settings.")
    c1, c2 = st.columns(2)
    current_co2 = float(current_row["Total_Energy_kWh"]) * float(emission_factor)
    golden_co2 = float(golden_profile.get("Carbon_kg", current_co2))
    c1.metric("Estimated CO2 for this batch (kg)", f"{current_co2:.2f}")
    c2.metric("Potential CO2 reduction vs golden (kg)", f"{(current_co2 - golden_co2):+.2f}")

# Bottom Row for all levels
st.subheader("What to do now")
recs = generate_adaptive_recommendations(current=current_row, golden_profile=golden_profile)
st.success(recs[0] if recs else "Keep current settings and continue monitoring.")
if mode != "Level 1 - Manual" and not is_simple:
    for rec in recs[1:3]:
        st.write(f"- {rec}")

history_summary = features[["Batch_ID", "Yield_Percent", "Quality_Score", "Total_Energy_kWh", "Carbon_kg", "Eco_Efficiency_Score", "Green_Zone"]].copy()
if is_simple:
    with st.expander("Recent batch summary", expanded=False):
        st.dataframe(history_summary.tail(8).reset_index(drop=True), use_container_width=True)
else:
    st.subheader("Historical Summary")
    st.dataframe(history_summary.tail(12).reset_index(drop=True), use_container_width=True)

# Enterprise add-ons
if mode == "Level 3 - Enterprise" and not is_simple and not bool(st.session_state.get("fast_start", False)):
    st.subheader("Enterprise decision tools")
    st.caption(f"Role: {role}. These sections help managers compare, decide, and report.")

    b1, b2 = st.columns(2)
    with b1:
        st.markdown("**Flexible comparison table**")
        st.caption("Compare a reference batch against Golden benchmark, Plant average, or another batch.")

        reference_batch = st.selectbox("Reference batch", options=features["Batch_ID"].tolist(), index=0, key="cmp_ref_batch")
        reference_row = features.loc[features["Batch_ID"] == reference_batch].iloc[0]

        compare_mode = st.radio(
            "Compare against",
            options=["Golden benchmark", "Plant average", "Another batch"],
            index=0,
            horizontal=True,
            key="cmp_mode",
        )

        comparison_label = "Golden benchmark"
        comparison_row = pd.Series(golden_profile)

        if compare_mode == "Plant average":
            comparison_label = "Plant average"
            comparison_row = pd.Series(
                {
                    "Yield_Percent": float(pd.to_numeric(features["Yield_Percent"], errors="coerce").mean()),
                    "Quality_Score": float(pd.to_numeric(features["Quality_Score"], errors="coerce").mean()),
                    "Total_Energy_kWh": float(pd.to_numeric(features["Total_Energy_kWh"], errors="coerce").mean()),
                    "Carbon_kg": float(pd.to_numeric(features["Carbon_kg"], errors="coerce").mean()),
                    "Eco_Efficiency_Score": float(pd.to_numeric(features["Eco_Efficiency_Score"], errors="coerce").mean()),
                }
            )
        elif compare_mode == "Another batch":
            other_batches = [b for b in features["Batch_ID"].tolist() if b != reference_batch]
            if other_batches:
                selected_other = st.selectbox("Comparison batch", options=other_batches, index=0, key="cmp_other_batch")
                comparison_label = f"Batch {selected_other}"
                comparison_row = features.loc[features["Batch_ID"] == selected_other].iloc[0]
            else:
                st.info("Only one batch exists in this dataset, so comparison is switched to Golden benchmark.")
                comparison_label = "Golden benchmark"
                comparison_row = pd.Series(golden_profile)

        def _safe_num(row: pd.Series, key: str, fallback: float = 0.0) -> float:
            val = pd.to_numeric(row.get(key, fallback), errors="coerce")
            if pd.isna(val):
                return float(fallback)
            return float(val)

        ref_yield = _safe_num(reference_row, "Yield_Percent")
        cmp_yield = _safe_num(comparison_row, "Yield_Percent", ref_yield)
        ref_quality = _safe_num(reference_row, "Quality_Score")
        cmp_quality = _safe_num(comparison_row, "Quality_Score", ref_quality)
        ref_energy = _safe_num(reference_row, "Total_Energy_kWh")
        cmp_energy = _safe_num(comparison_row, "Total_Energy_kWh", ref_energy)
        ref_carbon = _safe_num(reference_row, "Carbon_kg")
        cmp_carbon = _safe_num(comparison_row, "Carbon_kg", ref_carbon)
        ref_eco = _safe_num(reference_row, "Eco_Efficiency_Score")
        cmp_eco = _safe_num(comparison_row, "Eco_Efficiency_Score", ref_eco)

        comp_table = pd.DataFrame(
            [
                ["Yield %", ref_yield, cmp_yield, cmp_yield - ref_yield],
                ["Quality", ref_quality, cmp_quality, cmp_quality - ref_quality],
                ["Energy", ref_energy, cmp_energy, cmp_energy - ref_energy],
                ["Carbon", ref_carbon, cmp_carbon, cmp_carbon - ref_carbon],
                ["Eco Score", ref_eco, cmp_eco, cmp_eco - ref_eco],
            ],
            columns=["Metric", f"Reference ({reference_batch})", comparison_label, "Delta (Comparison-Reference)"],
        )
        st.caption("Delta interpretation: negative is better for Energy/Carbon, positive is better for Yield/Quality/Eco.")
        st.dataframe(comp_table, use_container_width=True)

    with b2:
        st.markdown("**System integration status**")
        st.caption("Health view of MES, ERP, Historian, and API connectivity.")
        status = pd.DataFrame(
            [
                ["MES Connector", "Simulated Connected", "Healthy"],
                ["ERP Connector", "Configured", "Sync every 4h"],
                ["Historian", "Connected", "Healthy"],
                ["API Gateway", "Connected", "Healthy"],
            ],
            columns=["System", "Status", "Note"],
        )
        st.table(status)

    if role in {"Manager", "Executive"}:
        # FEATURE 3: Enhanced Pareto Frontier — shows ALL batches (grey) +
        # Pareto-optimal frontier (green circles) + connecting line + regulatory limits
        st.markdown("**Best trade-off options (Pareto frontier)**")
        st.caption(
            "Grey dots = all feasible batches. 🟢 Green dots = Pareto-optimal (no batch is better"
            " on BOTH axes simultaneously). The green line connects the frontier. "
            "Red dashed lines = your regulatory targets."
        )
        pareto_input = optimizer.apply_targets(features, targets)
        if pareto_input.empty:
            pareto_input = features
        if len(pareto_input) > int(MAX_PARETO_ROWS):
            pareto_input = pareto_input.sample(n=int(MAX_PARETO_ROWS), random_state=42)
        pareto = optimizer.pareto_front(df=pareto_input)

        _reg_energy = float(st.session_state.get("reg_energy_target", 180.0))
        _reg_carbon = float(st.session_state.get("reg_carbon_target", 2.5))
        _industry_label = str(st.session_state.get("selected_industry", "Pharmaceutical"))
        _reg_std = str(st.session_state.get("reg_standard_label", ""))

        if not pareto.empty:
            # All batches — grey background
            all_batches_chart = (
                alt.Chart(pareto_input)
                .mark_circle(size=45, color="#4a6a8a", opacity=0.35)
                .encode(
                    x=alt.X("Total_Energy_kWh:Q", title="Energy (kWh)"),
                    y=alt.Y("Quality_Score:Q", title="Quality Score"),
                    tooltip=[
                        alt.Tooltip("Batch_ID:N", title="Batch"),
                        alt.Tooltip("Total_Energy_kWh:Q", title="Energy (kWh)", format=".2f"),
                        alt.Tooltip("Quality_Score:Q", title="Quality", format=".2f"),
                        alt.Tooltip("Yield_Percent:Q", title="Yield %", format=".2f"),
                        alt.Tooltip("Carbon_kg:Q", title="Carbon kg", format=".3f"),
                    ],
                )
            )
            # Pareto frontier dots — green
            pareto_dots = (
                alt.Chart(pareto.sort_values("Total_Energy_kWh"))
                .mark_circle(size=100, color="#22c98a", opacity=0.92)
                .encode(
                    x=alt.X("Total_Energy_kWh:Q"),
                    y=alt.Y("Quality_Score:Q"),
                    tooltip=[
                        alt.Tooltip("Batch_ID:N", title="Batch ⭐"),
                        alt.Tooltip("Total_Energy_kWh:Q", title="Energy (kWh)", format=".2f"),
                        alt.Tooltip("Quality_Score:Q", title="Quality", format=".2f"),
                        alt.Tooltip("Eco_Efficiency_Score:Q", title="Eco Score", format=".2f"),
                    ],
                )
            )
            # Frontier connecting line
            pareto_line = (
                alt.Chart(pareto.sort_values("Total_Energy_kWh"))
                .mark_line(color="#22c98a", strokeWidth=2, strokeDash=[4, 2])
                .encode(
                    x=alt.X("Total_Energy_kWh:Q"),
                    y=alt.Y("Quality_Score:Q"),
                )
            )
            # Regulatory energy limit line
            reg_energy_rule = (
                alt.Chart(pd.DataFrame([{"reg_energy": _reg_energy}]))
                .mark_rule(color="#ff6b6b", strokeDash=[5, 3], strokeWidth=2)
                .encode(x=alt.X("reg_energy:Q", title="Energy (kWh)"))
            )
            pareto_combined = (
                alt.layer(all_batches_chart, pareto_line, pareto_dots, reg_energy_rule)
                .properties(height=340)
                .configure_view(stroke=None)
                .configure_axis(
                    labelFontSize=12, titleFontSize=13,
                    titleColor="#eaf4ff", labelColor="#d8e8f6",
                    gridColor="#254a71", domainColor="#2d5a83",
                )
            )
            st.altair_chart(pareto_combined, use_container_width=True)
            st.caption(
                f"🔴 Red line = {_industry_label} energy limit ({_reg_energy:.0f} kWh, {_reg_std}). "
                f"Batches to the left of this line are **regulatory-compliant**."
            )
            _pareto_compliant = int((pareto["Total_Energy_kWh"] <= _reg_energy).sum())
            st.info(f"✅ **{_pareto_compliant} of {len(pareto)} Pareto-optimal batches** are within {_industry_label} regulatory energy limit ({_reg_energy:.0f} kWh).")
        else:
            st.info("No trade-off points available for current filters. Try relaxing strictness.")

    if role == "Executive":
        st.markdown("**Best benchmark trend over time**")
        st.caption("Shows whether our golden benchmark score is improving month by month.")
        if history_path.exists():
            try:
                hist_df = pd.read_csv(history_path)
            except Exception as exc:
                logger.exception("Failed to read history file for trend chart: %s", history_path)
                st.warning("Unable to load history trend due to a file read issue.")
                hist_df = pd.DataFrame()
            hist_df["timestamp"] = pd.to_datetime(hist_df["timestamp"], errors="coerce")
            hist_goal = hist_df[hist_df["scenario"] == goal].copy()
            if not hist_goal.empty:
                evolution = (
                    alt.Chart(hist_goal.sort_values("timestamp"))
                    .mark_line(point=True, color="#9bd9ff", strokeWidth=2.5)
                    .encode(
                        x=alt.X("timestamp:T", title="Time"),
                        y=alt.Y("score:Q", title="Golden Score"),
                        tooltip=["timestamp:T", alt.Tooltip("score:Q", format=".2f")],
                    )
                    .properties(height=250)
                )
                st.altair_chart(evolution, use_container_width=True)
            else:
                st.info("No evolution history available yet.")
        else:
            st.info("No evolution history available yet.")

        # FEATURE 4: Richer Management Report Export with regulatory compliance +
        # recommendations + ROI + HITL decision log
        st.markdown("**Management report export**")
        st.caption("Download full executive KPI report — includes regulatory compliance status, ROI, and AI recommendations.")
        roi = estimate_roi(
            current_energy_kwh=float(current_row["Total_Energy_kWh"]),
            golden_energy_kwh=float(golden_profile.get("Total_Energy_kWh", current_row["Total_Energy_kWh"])),
            energy_cost_per_kwh=float(energy_cost),
            annual_batches=int(annual_batches),
        )
        _report_industry = str(st.session_state.get("selected_industry", "N/A"))
        _report_reg_energy = float(st.session_state.get("reg_energy_target", 180.0))
        _report_reg_carbon = float(st.session_state.get("reg_carbon_target", 2.5))
        _report_reg_std = str(st.session_state.get("reg_standard_label", ""))
        _batch_energy = float(current_row["Total_Energy_kWh"])
        _batch_carbon = float(current_row.get("Carbon_kg", _batch_energy * float(emission_factor)))
        _energy_compliant = "✅ COMPLIANT" if _batch_energy <= _report_reg_energy else "❌ NON-COMPLIANT"
        _carbon_compliant = "✅ COMPLIANT" if _batch_carbon <= _report_reg_carbon else "❌ NON-COMPLIANT"

        # Rich metrics panel
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Annual Savings (USD)", f'${float(roi["annual_savings_usd"]):,.2f}')
        r2.metric(f"Energy vs {_report_industry} limit", _energy_compliant.split()[0])
        r3.metric(f"Carbon vs {_report_industry} limit", _carbon_compliant.split()[0])
        r4.metric("Energy delta vs Golden", f'{roi["delta_energy_kwh"]:+.3f} kWh')

        # Recommendation text for PDF
        _recs_for_report = generate_adaptive_recommendations(current=current_row, golden_profile=golden_profile)

        kpi_export = pd.DataFrame(
            [
                ["Report Generated (UTC)", pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M UTC")],
                ["Factory Mode", mode],
                ["Industry Sector", _report_industry],
                ["Regulatory Standard", _report_reg_std],
                ["Role", role],
                ["Optimization Goal", goal],
                ["Current Batch", current_batch_id],
                ["── PERFORMANCE ──", ""],
                ["Current Energy (kWh)", f'{_batch_energy:.2f}'],
                ["Current Quality Score", f'{float(current_row["Quality_Score"]):.2f}'],
                ["Current Yield %", f'{float(current_row.get("Yield_Percent", 0)):.2f}'],
                ["Current Eco Score", f'{float(current_row["Eco_Efficiency_Score"]):.2f}'],
                ["Current Carbon (kg)", f'{_batch_carbon:.3f}'],
                ["── REGULATORY COMPLIANCE ──", ""],
                [f"Energy limit ({_report_industry})", f'{_report_reg_energy:.1f} kWh'],
                ["Energy compliance", _energy_compliant],
                [f"Carbon limit ({_report_industry})", f'{_report_reg_carbon:.2f} kg'],
                ["Carbon compliance", _carbon_compliant],
                ["── ROI ──", ""],
                ["Energy delta vs Golden (kWh)", f'{roi["delta_energy_kwh"]:+.3f}'],
                ["Savings per batch (USD)", f'${roi["savings_per_batch_usd"]:.2f}'],
                ["Annual savings (USD)", f'${float(roi["annual_savings_usd"]):,.2f}'],
                ["── AI RECOMMENDATIONS ──", ""],
            ] + [[f"Rec {i+1}", r] for i, r in enumerate(_recs_for_report[:5])],
            columns=["KPI", "Value"],
        )
        csv_bytes = sanitize_csv(kpi_export)
        st.download_button(
            "📥 Download Executive CSV", csv_bytes,
            file_name=f"agpo_executive_report_{pd.Timestamp.utcnow().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
        pdf_lines = [
            "AGPO — Executive Report",
            f"Generated: {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            "",
        ] + [f"{row.KPI}: {row.Value}" for row in kpi_export.itertuples(index=False)]
        pdf_bytes = to_pdf_bytes(pdf_lines)
        st.download_button(
            "📄 Download Executive PDF", pdf_bytes,
            file_name=f"agpo_executive_report_{pd.Timestamp.utcnow().strftime('%Y%m%d')}.pdf",
            mime="application/pdf",
        )
        st.subheader(tr("Admin Panel"))
        scores_all = optimizer.rank_batches(weights=weights, targets=targets)["Scenario_Score"]
        ai_conf = float(max(0.0, 100.0 - float(scores_all.std()) * 2.0))
        c1, c2, c3 = st.columns(3)
        c1.metric(tr("AI Confidence"), f"{ai_conf:.1f}%")
        c2.metric(tr("Avg Scenario Score"), f"{float(scores_all.mean()):.1f}")
        c3.metric(tr("Score StdDev"), f"{float(scores_all.std()):.2f}")
        users = []
        for token in str(AUTH_USERS).split(","):
            parts = [p.strip() for p in token.split(":")]
            if len(parts) == 3:
                users.append({"username": parts[0], "role": parts[2]})
        if users:
            st.caption(tr("Users"))
            st.table(pd.DataFrame(users))
        st.caption(tr("Security Events"))
        sec_log = Path("artifacts/security_events.log")
        if sec_log.exists():
            lines = sec_log.read_text(encoding="utf-8").splitlines()[-50:]
            st.text("\n".join(lines))
        block = st.toggle(tr("Block uploads in session"), value=bool(st.session_state.get("uploads_blocked_session", False)))
        st.session_state["uploads_blocked_session"] = bool(block)
elif mode == "Level 3 - Enterprise":
    st.info("Enterprise analytics are hidden in Simple mode. Switch to Advanced when needed.")

st.caption(
    f"Cache: {'Hit' if cache_info.get('cache_hit') else 'Rebuilt'} | "
    f"Rows: {len(features)} | Signature: {cache_info.get('signature')}"
)
