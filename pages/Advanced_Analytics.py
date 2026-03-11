import streamlit as st

st.set_page_config(page_title="Advanced Analytics - Pollution Radar", layout="wide")

st.title("Advanced Analytics")
st.write("Detailed pollution data and trends.")

if st.button("Go back to Home"):
    st.switch_page("app.py")
