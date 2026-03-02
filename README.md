# AGPO: Universal Factory Optimization Platform

This project is a Track B prototype redesigned as a **single modular platform** that scales from manual factories to SCADA/MES environments.

## Core Positioning

One software intelligence layer, multiple ingestion adapters:

Data Source -> Data Normalization Layer -> Golden Signature Engine -> Deviation Engine -> Recommendation Engine -> Dashboard/API

## What Is Implemented

1. **Universal Ingestion (Pluggable Adapters)**
- Historical Excel adapter (your provided datasets)
- Manual form adapter (zero-digital factories)
- CSV/Excel upload adapter (semi-digital factories)
- SCADA snapshot adapter (advanced factories)

2. **Standard Data Schema**
- Normalizes all sources into one schema:
  - `timestamp`
  - `batch_id`
  - `temperature_c`
  - `pressure_bar`
  - `rpm`
  - `energy_kwh`
  - `yield_percent`
  - `quality_score`
  - `process_time_min`
  - `carbon_kg`

3. **Golden Signature + Multi-Objective Optimization**
- Scenario-based golden signatures
- Pareto front generation
- Weighted optimization across:
  - Yield
  - Quality
  - Performance
  - Energy
  - Carbon
  - Eco Efficiency

4. **Eco Layer (Pollution-Aware)**
- Carbon estimation from energy
- `Eco_Efficiency_Score` feature
- `Green_Zone` indicator: `Red`, `Yellow`, `Green`
- Green-priority optimization scenario

5. **Premium Dark UI**
- Custom dark theme with matching accent palette (teal + amber)
- Glass-style metric cards, animated section chips, responsive layout
- Optimized readability for desktop and mobile demo screens

6. **Real-time Comparison + Adaptive Correction**
- Current batch vs golden signature deviation
- Parameter and eco-aware recommendation output
- ROI view (per-batch and annual savings)

7. **Continuous Learning**
- Simulated better batch
- Automatic golden-signature promotion if better score is detected

8. **Large-Scale Readiness**
- Persistent feature-store cache (`artifacts/feature_store`)
- Signature-based cache invalidation when source files change
- Controlled table rendering limits for responsive dashboards on large datasets

## Project Structure

```text
app.py
trackb_engine/
  adapters.py
  config.py
  data_pipeline.py
  feature_store.py
  optimization.py
  golden.py
  realtime.py
  learning.py
artifacts/
.streamlit/
requirements.txt
```

## Run

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Required Dataset Files

Keep these files in the project root:
- `_h_batch_production_data.xlsx`
- `_h_batch_process_data_copy.xlsx`
