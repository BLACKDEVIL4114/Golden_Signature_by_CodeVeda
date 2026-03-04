"""Global configuration and objective setup for Track B."""

from __future__ import annotations

from typing import Dict
import os

from dotenv import load_dotenv

load_dotenv()

PRODUCTION_DATA_FILE = os.environ.get("PRODUCTION_DATA_FILE", "_h_batch_production_data.xlsx")
PROCESS_DATA_FILE = os.environ.get("PROCESS_DATA_FILE", "_h_batch_process_data_copy.xlsx")
GOLDEN_SIGNATURE_FILE = os.environ.get("GOLDEN_SIGNATURE_FILE", "artifacts/golden_signatures.json")

DEFAULT_EMISSION_FACTOR = float(os.environ.get("EMISSION_FACTOR", "0.82"))
DEFAULT_ENERGY_COST = float(os.environ.get("ENERGY_COST", "0.12"))
DEFAULT_ANNUAL_BATCHES = int(os.environ.get("ANNUAL_BATCHES", "1200"))

FEATURE_STORE_DIR = os.environ.get("FEATURE_STORE_DIR", "artifacts/feature_store")
FEATURE_STORE_UPLOADS_DIR = os.environ.get("FEATURE_STORE_UPLOADS_DIR", "artifacts/feature_store_uploaded")
# SECURITY FIX 6: Reduce default upload cap from 200 MB to 50 MB.
# The original 200 MB default was 4× larger than documented in .env.example
# and creates a DoS vector.  Operators can override via MAX_UPLOAD_BYTES env var.
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(50 * 1024 * 1024)))
ALLOWED_UPLOAD_EXTS = set([ext.strip().lower() for ext in os.environ.get("ALLOWED_UPLOAD_EXTS", "xlsx,csv").split(",") if ext.strip()])
MAX_INPUT_ROWS = int(os.environ.get("MAX_INPUT_ROWS", "200000"))
SECURITY_LOG_FILE = os.environ.get("SECURITY_LOG_FILE", "artifacts/security_events.log")
AUTH_ENABLED = os.environ.get("AUTH_ENABLED", "false").lower() == "true"
AUTH_USERS = os.environ.get("AUTH_USERS", "")
ALERT_ENABLED = os.environ.get("ALERT_ENABLED", "false").lower() == "true"
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "")
ALERT_SMTP_SERVER = os.environ.get("ALERT_SMTP_SERVER", "")
ALERT_SMTP_PORT = int(os.environ.get("ALERT_SMTP_PORT", "587"))
ALERT_SMTP_USER = os.environ.get("ALERT_SMTP_USER", "")
ALERT_SMTP_PASS = os.environ.get("ALERT_SMTP_PASS", "")

# FIX S1: Fail fast if auth is enabled but no users are configured.
if AUTH_ENABLED and not AUTH_USERS.strip():
    import warnings as _warnings
    _warnings.warn(
        "[SECURITY] AUTH_ENABLED is true but AUTH_USERS environment variable is not set. "
        "Set AUTH_USERS=username:password:Role to configure valid credentials.",
        stacklevel=1,
    )
UPLOAD_RATE_LIMIT = int(os.environ.get("UPLOAD_RATE_LIMIT", "8"))
UPLOAD_RATE_WINDOW_SEC = int(os.environ.get("UPLOAD_RATE_WINDOW_SEC", "600"))
BACKOFF_MAX_SECONDS = int(os.environ.get("BACKOFF_MAX_SECONDS", "300"))
MAX_NULL_RATE = float(os.environ.get("MAX_NULL_RATE", "0.55"))
ENERGY_MAX_KWH = float(os.environ.get("ENERGY_MAX_KWH", "1000000"))
MAX_PARETO_ROWS = int(os.environ.get("MAX_PARETO_ROWS", "10000"))

STANDARD_SCHEMA = [
    "timestamp",
    "batch_id",
    "temperature_c",
    "pressure_bar",
    "rpm",
    "energy_kwh",
    "yield_percent",
    "quality_score",
    "process_time_min",
    "carbon_kg",
]

FACTORY_LEVEL_PROFILES = {
    "Level 1: Manual / Zero-Digital": {
        "ingestion": "Manual form or Excel upload",
        "integration": "No hardware integration required",
    },
    "Level 2: Semi-Digital": {
        "ingestion": "CSV auto-import or IoT gateway export",
        "integration": "Basic PLC/HMI/Modbus connectors",
    },
    "Level 3: Advanced / SCADA-MES": {
        "ingestion": "OPC-UA, SQL, REST API streams",
        "integration": "Full historian + enterprise integrations",
    },
}

PRIMARY_OBJECTIVES = [
    "Yield_Percent",
    "Quality_Score",
    "Performance_Score",
    "Total_Energy_kWh",
    "Carbon_kg",
    "Eco_Efficiency_Score",
]

OBJECTIVE_DIRECTIONS = {
    "Yield_Percent": "max",
    "Quality_Score": "max",
    "Performance_Score": "max",
    "Total_Energy_kWh": "min",
    "Carbon_kg": "min",
    "Eco_Efficiency_Score": "max",
}

# Generic control parameters — apply to ANY manufacturing industry
# Pharma: Granulation_Time, Binder_Amount, Drying_Temp | Textile: Loom_Speed, Dye_Amount, Cure_Temp
# Food: Mix_Time, Ingredient_Amount, Cook_Temp | Auto: Cycle_Time, Lubricant, Press_Temp
CONTROL_PARAMETERS = [
    "Cycle_Time",           # pharma: Granulation_Time | textile: Mix_Time | food: Cook_Time
    "Process_Agent_Amount", # pharma: Binder_Amount | textile: Dye_Amount | food: Ingredient_Amount
    "Heat_Temp",            # pharma: Drying_Temp | textile: Cure_Temp | food: Cook_Temp
    "Heat_Duration",        # pharma: Drying_Time | textile: Cure_Time | food: Cook_Duration
    "Press_Force",          # pharma: Compression_Force | auto: Stamp_Force | plastics: Clamp_Force
    "Machine_Speed",        # universal: RPM / line speed / conveyor speed
    "Lubricant_Additive",   # pharma: Lubricant_Conc | auto: Oil_Conc | plastics: Release_Agent
    "Moisture_Level",       # universal: moisture / humidity at output stage
]

DEFAULT_SCENARIOS: Dict[str, Dict[str, float]] = {
    "Balanced": {
        "Yield_Percent": 0.22,
        "Quality_Score": 0.20,
        "Performance_Score": 0.16,
        "Total_Energy_kWh": 0.14,
        "Carbon_kg": 0.12,
        "Eco_Efficiency_Score": 0.16,
    },
    "Best Yield + Low Energy": {
        "Yield_Percent": 0.36,
        "Quality_Score": 0.10,
        "Performance_Score": 0.05,
        "Total_Energy_kWh": 0.21,
        "Carbon_kg": 0.11,
        "Eco_Efficiency_Score": 0.17,
    },
    "Best Quality + Best Yield": {
        "Yield_Percent": 0.29,
        "Quality_Score": 0.34,
        "Performance_Score": 0.08,
        "Total_Energy_kWh": 0.08,
        "Carbon_kg": 0.08,
        "Eco_Efficiency_Score": 0.13,
    },
    "Max Performance + Min Carbon": {
        "Yield_Percent": 0.06,
        "Quality_Score": 0.08,
        "Performance_Score": 0.34,
        "Total_Energy_kWh": 0.12,
        "Carbon_kg": 0.22,
        "Eco_Efficiency_Score": 0.18,
    },
    "Green Compliance Priority": {
        "Yield_Percent": 0.14,
        "Quality_Score": 0.10,
        "Performance_Score": 0.08,
        "Total_Energy_kWh": 0.23,
        "Carbon_kg": 0.22,
        "Eco_Efficiency_Score": 0.23,
    },
}
