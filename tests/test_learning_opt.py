import unittest
import pandas as pd
from trackb_engine.learning import simulate_improved_candidate
from trackb_engine.config import CONTROL_PARAMETERS, PRIMARY_OBJECTIVES, DEFAULT_EMISSION_FACTOR

class TestLearningOptimization(unittest.TestCase):
    def _build_fake_features(self, rows=60):
        data = {}
        for col in CONTROL_PARAMETERS:
            data[col] = pd.Series(range(1, rows + 1)).astype(float)
        data["Batch_ID"] = [f"B{i:03d}" for i in range(rows)]
        for obj in PRIMARY_OBJECTIVES:
            data[obj] = pd.Series(range(1, rows + 1)).astype(float)
        return pd.DataFrame(data)

    def test_surrogate_resets_and_optimizes(self):
        f1 = self._build_fake_features(60)
        gold = f1.iloc[0].to_dict()
        cand1 = simulate_improved_candidate(gold, DEFAULT_EMISSION_FACTOR, feature_table=f1)
        self.assertTrue(str(cand1.get("Batch_ID", "")).startswith("SIM_"))
        f2 = self._build_fake_features(80)
        cand2 = simulate_improved_candidate(gold, DEFAULT_EMISSION_FACTOR, feature_table=f2)
        self.assertTrue(str(cand2.get("Batch_ID", "")).startswith("SIM_"))

if __name__ == "__main__":
    unittest.main()
