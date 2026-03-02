import unittest
import pandas as pd
from trackb_engine.realtime import sanitize_csv


class TestSecurity(unittest.TestCase):
    def test_sanitize_csv_formula_injection(self):
        df = pd.DataFrame(
            {
                "KPI": ["Safe", "Formula", "Negative", "AtSign", "Tab"],
                "Value": ["hello", "=SUM(1,2)", "-1", "@cmd", "\tstart"],
            }
        )
        out = sanitize_csv(df).decode("utf-8")
        self.assertIn("Safe,hello", out)
        self.assertIn("'=SUM(1,2)", out)
        self.assertIn("Negative,'-1", out)
        self.assertIn("AtSign,'@cmd", out)
        self.assertIn("Tab,'\tstart", out)


if __name__ == "__main__":
    unittest.main()
