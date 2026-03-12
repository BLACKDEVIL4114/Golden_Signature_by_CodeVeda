from __future__ import annotations

import streamlit as st


st.set_page_config(
    page_title="Factory Sustainability FAQ - Pollution Radar",
    page_icon="❓",
    layout="wide",
)

st.title("Factory Sustainability FAQ")
st.caption("Common questions about industrial pollution tracking, emissions monitoring, and sustainability dashboards.")

FAQS = [
    (
        "What is industrial pollution monitoring?",
        "Industrial pollution monitoring is the process of tracking emissions, waste, energy use, and process conditions so factories can detect environmental risks early and improve operational efficiency.",
    ),
    (
        "Can small factories monitor emissions without SCADA systems?",
        "Yes. Small factories can start with manual records, Excel sheets, CSV uploads, and periodic meter readings, then move toward more automated telemetry later.",
    ),
    (
        "Why is a sustainability dashboard useful for manufacturing?",
        "A sustainability dashboard connects energy, carbon, yield, and quality in one place so teams can make faster decisions with fewer blind spots.",
    ),
    (
        "What is a golden signature in manufacturing analytics?",
        "A golden signature is a benchmark profile built from strong-performing batches. It helps operators compare current runs against the conditions associated with better results.",
    ),
    (
        "How do factories reduce carbon emissions quickly?",
        "The fastest wins usually come from identifying energy-heavy process deviations, stabilizing process parameters, reducing rework, and repeating the best-performing low-energy batches.",
    ),
]

for question, answer in FAQS:
    with st.expander(question, expanded=False):
        st.write(answer)

st.header("Next steps for plant teams")
st.markdown(
    """
- Track baseline energy and carbon data.
- Build a standard process dashboard.
- Compare every new batch to your benchmark profile.
- Review hotspots weekly and correct them before they become recurring waste.
"""
)
