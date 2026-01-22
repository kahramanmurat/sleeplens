import unittest
from pyspark.sql import SparkSession
from processing.spark.quality_checks import run_quality_checks
import shutil
import os

class TestQualityChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestQualityChecks").master("local[1]").getOrCreate()
        cls.test_data_path = "test_data_qc"
        os.makedirs(cls.test_data_path, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree(cls.test_data_path)

    def test_quality_checks_pass(self):
        # Create valid data
        data = [{"study_id": "1", "duration_sec": 100}, {"study_id": "2", "duration_sec": 200}]
        df = self.spark.createDataFrame(data)
        path = f"{self.test_data_path}/valid.parquet"
        df.write.parquet(path)
        
        self.assertTrue(run_quality_checks(self.spark, path))

    def test_quality_checks_fail_null_id(self):
        # Create invalid data
        data = [{"study_id": None, "duration_sec": 100}]
        df = self.spark.createDataFrame(data)
        path = f"{self.test_data_path}/invalid_null.parquet"
        df.write.parquet(path)
        
        self.assertFalse(run_quality_checks(self.spark, path))

if __name__ == '__main__':
    unittest.main()
