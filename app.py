import streamlit as st

st.set_page_config(page_title="Pollution Radar", layout="wide")

st.title("Welcome to Pollution Radar")
st.write("Real-time pollution monitoring and analysis.")

st.markdown("---")
st.subheader("Sitemap")
st.write("You can access the sitemap at:")
st.markdown("[/static/sitemap.xml](/static/sitemap.xml)", unsafe_allow_html=True)

st.info("To serve this at the root /sitemap.xml on Streamlit Cloud, consider using a proxy like Cloudflare to redirect from `/sitemap.xml` to `/static/sitemap.xml`.")
