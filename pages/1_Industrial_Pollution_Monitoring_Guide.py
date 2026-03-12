from __future__ import annotations

import streamlit as st


st.set_page_config(
    page_title="Industrial Pollution Monitoring Guide - Pollution Radar",
    page_icon="🏭",
    layout="wide",
)

st.title("Industrial Pollution Monitoring Guide")
st.caption("A practical guide to tracking emissions, energy waste, and eco-performance in factories.")

st.markdown(
    """
Industrial pollution monitoring helps factories track emissions, energy use, water discharge,
and process deviations before they become compliance, cost, or sustainability problems. A
modern monitoring workflow combines real-time data, clear thresholds, and action-oriented
recommendations so plant teams can respond quickly.
"""
)

st.header("Why factories need continuous monitoring")
st.markdown(
    """
- Detect abnormal emissions before they escalate into production or compliance incidents.
- Reduce energy waste that directly increases operating cost and carbon output.
- Compare current performance against best-performing batches or baseline process signatures.
- Give plant managers one dashboard for yield, quality, carbon, and eco-efficiency.
"""
)

st.header("Core metrics to monitor")
st.markdown(
    """
- Air emissions and particulate trends
- Energy consumption by batch or process stage
- Carbon emissions estimated from production energy use
- Process parameters such as temperature, pressure, RPM, and cycle time
- Quality and yield losses caused by unstable process conditions
"""
)

st.header("Recommended monitoring workflow")
st.markdown(
    """
1. Collect process and production data from manual logs, CSV uploads, or SCADA exports.
2. Normalize the data into one clean schema for comparison.
3. Build a golden signature from your best-performing batches.
4. Measure live deviations against that benchmark.
5. Trigger corrective actions for energy, quality, and carbon hotspots.
"""
)

st.header("How Pollution Radar helps")
st.markdown(
    """
Pollution Radar is designed for factories that need one place to monitor process efficiency,
carbon impact, and pollution risk. It supports multiple maturity levels, from manual factories
to advanced plants with structured telemetry.
"""
)

st.markdown(
    """
Useful next pages:

- [Carbon Emission Monitoring for Factories](./Carbon_Emission_Monitoring_for_Factories)
- [Factory Sustainability FAQ](./Factory_Sustainability_FAQ)
- [Advanced Analytics](./Advanced_Analytics)
""",
    unsafe_allow_html=True,
)
