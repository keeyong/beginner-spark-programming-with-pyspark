"""
 - python -m unittest test_df.py
"""
from unittest import TestCase
from pyspark.sql import SparkSession
from my_df import load_gender, get_gender_count, upper_udf_f

class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .appName("Spark Unit Test") \
            .getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_gender(self.spark, "name_gender.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 100, "Record count should be 100")

    def test_gender_count(self):
        sample_df = load_gender(self.spark, "name_gender.csv")
        count_list = get_gender_count(self.spark, sample_df, "gender").collect()
        count_dict = dict()
        for row in count_list:
            count_dict[row["gender"]] = row["count"]
        self.assertEqual(count_dict["F"], 65, "Count for F should be 65")
        self.assertEqual(count_dict["M"], 28, "Count for M should be 28")
        self.assertEqual(count_dict["Unisex"], 7, "Count for Unisex should be 7")

    def test_upper_udf(self):
        test_data = [
            { "name": "John Kim" },
            { "name": "Johnny Kim"},
            { "name": "1234" }
        ]
        expected_results = [ "JOHN KIM", "JOHNNY KIM", "1234" ]

        upperUDF = self.spark.udf.register("upper_udf", upper_udf_f)
        test_df = self.spark.createDataFrame(test_data)
        names = test_df.select("name", upperUDF("name").alias("NAME")).collect()
        results = []
        for name in names:
            results.append(name["NAME"])
        self.assertCountEqual(results, expected_results)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

