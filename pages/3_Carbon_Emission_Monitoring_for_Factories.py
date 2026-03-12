from __future__ import annotations

import streamlit as st


st.set_page_config(
    page_title="Carbon Emission Monitoring for Factories - Pollution Radar",
    page_icon="🌿",
    layout="wide",
)

st.title("Carbon Emission Monitoring for Factories")
st.caption("How manufacturers can measure, reduce, and report production-related carbon impact.")

st.markdown(
    """
Carbon emission monitoring gives factory teams a direct view of how production decisions affect
energy cost, sustainability targets, and environmental reporting. The most useful approach is
to calculate carbon impact alongside throughput and quality, not as a separate afterthought.
"""
)

st.header("What should be tracked")
st.markdown(
    """
- Total energy consumed per batch
- Estimated carbon kilograms based on your emission factor
- Carbon per unit produced
- Eco-efficiency score compared with baseline runs
- High-energy process steps that drive avoidable emissions
"""
)

st.header("Why carbon visibility matters")
st.markdown(
    """
- Rising energy use often signals process inefficiency.
- Carbon trends help prioritize green improvement projects.
- Sustainability reporting becomes easier when emissions are tied to actual production records.
- Operations teams can compare tradeoffs between output, quality, and emissions.
"""
)

st.header("Best practices")
st.markdown(
    """
1. Start with reliable energy data, even if it comes from batch sheets or uploaded files.
2. Use one emission factor consistently across reporting periods.
3. Compare current batches to your best low-carbon production runs.
4. Investigate process drift early rather than after a monthly review.
5. Share carbon results with production, maintenance, and leadership together.
"""
)

st.header("How Pollution Radar supports low-carbon manufacturing")
st.markdown(
    """
Pollution Radar estimates carbon from operational data, highlights greener process zones, and
shows where energy-intensive batches deviate from stronger benchmarks. That makes it easier to
cut waste without sacrificing yield or quality.
"""
)
