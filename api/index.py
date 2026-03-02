import importlib.util
import pathlib

# Load the root-level api.py without clashing with this 'api' package
_root = pathlib.Path(__file__).resolve().parent.parent
_api_py = _root / "api.py"
_spec = importlib.util.spec_from_file_location("api_root", str(_api_py))
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)

# Expose FastAPI app for Vercel
app = _mod.app
