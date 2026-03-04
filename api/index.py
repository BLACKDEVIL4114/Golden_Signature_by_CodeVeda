import importlib.util
from pathlib import Path
_root = Path(__file__).resolve().parent.parent
_api_py = _root / "api.py"
_spec = importlib.util.spec_from_file_location("agpo_api_root", str(_api_py))
_mod = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_mod)
app = _mod.app
