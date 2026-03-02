from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .config import SECURITY_LOG_FILE


def _sanitize_log_details(d: dict) -> dict:
    """Sanitize log values to prevent log injection attacks."""
    return {str(k)[:64]: str(v)[:256] for k, v in (d or {}).items()}


def log_event(event: str, details: Dict[str, Any] | None = None) -> None:
    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "event": str(event)[:128],
        "details": _sanitize_log_details(details or {}),
    }
    path = Path(SECURITY_LOG_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    max_bytes = 5 * 1024 * 1024
    if path.exists():
        try:
            if path.stat().st_size >= max_bytes:
                backup = path.with_suffix(path.suffix + ".1")
                try:
                    if backup.exists():
                        backup.unlink()
                except Exception:
                    pass
                path.rename(backup)
        except Exception:
            pass
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
