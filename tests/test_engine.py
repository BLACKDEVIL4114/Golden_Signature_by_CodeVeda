import unittest
from pathlib import Path
import pandas as pd

from trackb_engine.config import (
    PRODUCTION_DATA_FILE,
    PROCESS_DATA_FILE,
    DEFAULT_EMISSION_FACTOR,
    FEATURE_STORE_DIR,
)
from trackb_engine.data_pipeline import run_pipeline
from trackb_engine.feature_store import load_or_build_pipeline


class TestEngine(unittest.TestCase):
    def test_run_pipeline_demo_files(self):
        prod = str(Path(PRODUCTION_DATA_FILE).resolve())
        proc = str(Path(PROCESS_DATA_FILE).resolve())
        artifacts = run_pipeline(prod, proc, emission_factor=DEFAULT_EMISSION_FACTOR)
        f = artifacts.features
        self.assertIsInstance(f, pd.DataFrame)
        self.assertGreater(len(f), 0)
        for col in ["Batch_ID", "Total_Energy_kWh", "Quality_Score", "Yield_Percent", "Eco_Efficiency_Score"]:
            self.assertIn(col, f.columns)

    def test_feature_store_cache_roundtrip(self):
        prod = str(Path(PRODUCTION_DATA_FILE).resolve())
        proc = str(Path(PROCESS_DATA_FILE).resolve())
        artifacts1, info1 = load_or_build_pipeline(prod, proc, DEFAULT_EMISSION_FACTOR, cache_dir=FEATURE_STORE_DIR, use_store=True, force_rebuild=True)
        artifacts2, info2 = load_or_build_pipeline(prod, proc, DEFAULT_EMISSION_FACTOR, cache_dir=FEATURE_STORE_DIR, use_store=True, force_rebuild=False)
        self.assertFalse(info1.get("cache_hit", False))
        self.assertTrue(info2.get("cache_hit", False))
        self.assertEqual(info1.get("cache_dir"), info2.get("cache_dir"))
        self.assertEqual(set(artifacts1.features.columns), set(artifacts2.features.columns))

    def test_data_mode_flag_present(self):
        prod = str(Path(PRODUCTION_DATA_FILE).resolve())
        proc = str(Path(PROCESS_DATA_FILE).resolve())
        artifacts = run_pipeline(prod, proc, emission_factor=DEFAULT_EMISSION_FACTOR)
        mode = artifacts.cleaning_report.get("data_mode")
        self.assertIn(mode, {"Full", "Partial", "Minimal"})


if __name__ == "__main__":
    unittest.main()
