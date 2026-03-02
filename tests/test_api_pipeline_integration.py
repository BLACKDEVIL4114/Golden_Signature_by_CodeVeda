import unittest
from pathlib import Path
from fastapi.testclient import TestClient
from trackb_engine.config import PRODUCTION_DATA_FILE, PROCESS_DATA_FILE
import api as agpo_api

class TestApiPipelineIntegration(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(agpo_api.app)

    def test_pipeline_run_demo(self):
        prod = str(Path(PRODUCTION_DATA_FILE).resolve())
        proc = str(Path(PROCESS_DATA_FILE).resolve())
        resp = self.client.post("/pipeline/run", json={
            "production_file": prod,
            "process_file": proc,
            "emission_factor": 0.82,
            "use_feature_store": True,
            "force_rebuild": False,
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertGreater(int(data["features_count"]), 0)
        self.assertIn("cleaning_report", data)

    def test_pipeline_run_path_traversal_rejected(self):
        resp = self.client.post("/pipeline/run", json={
            "production_file": "../../../etc/passwd",
            "process_file": "../../../etc/shadow",
            "emission_factor": 0.82,
            "use_feature_store": False,
            "force_rebuild": False,
        })
        self.assertEqual(resp.status_code, 400)

if __name__ == "__main__":
    unittest.main()
