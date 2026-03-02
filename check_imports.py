import traceback, sys

print("=== Import Test ===")
try:
    from trackb_engine import config
    print("config OK")
    from trackb_engine import telemetry
    print("telemetry OK")
    from trackb_engine import golden
    print("golden OK")
    from trackb_engine import feature_store
    print("feature_store OK")
    from trackb_engine import adapters
    print("adapters OK")
    from trackb_engine import realtime
    print("realtime OK")
    from trackb_engine import optimization
    print("optimization OK")
    import altair
    print("altair OK")
    import streamlit
    print("streamlit OK")
    print("=== ALL IMPORTS OK ===")
except Exception:
    print("=== IMPORT FAILED ===")
    traceback.print_exc()
