import unittest
from pathlib import Path
import json
from trackb_engine.telemetry import log_event
from trackb_engine.config import SECURITY_LOG_FILE


class TestTelemetry(unittest.TestCase):
    def test_log_event_writes_jsonl(self):
        path = Path(SECURITY_LOG_FILE)
        if path.exists():
            path.unlink()
        log_event("unit_test_event", {"a": 1})
        self.assertTrue(path.exists())
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        self.assertGreaterEqual(len(lines), 1)
        payload = json.loads(lines[-1])
        self.assertEqual(payload["event"], "unit_test_event")
        self.assertEqual(payload["details"]["a"], 1)


if __name__ == "__main__":
    unittest.main()
