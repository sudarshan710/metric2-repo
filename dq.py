# import os
# os.environ['SPARK_VERSION'] = '3.3'

# from pydeequ.checks import Check, CheckLevel
# from pydeequ.verification import VerificationSuite
# from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .appName("DataQualityChecks")
#     .config("spark.jars", "jars/deequ-2.0.3-spark-3.3.jar,jars/scala-library-2.12.15.jar")
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

# data = spark.read.csv("customers-100.csv", header=True, inferSchema=True)
# check = Check(spark, CheckLevel.Error, "Data quality checks").hasSize(lambda x: x >= 100)

# result = VerificationSuite(spark).onData(data).addCheck(check).run()

# if result.status != "Success":
#     print("Data quality checks failed. Blocking deployment.")

# if result.status != "Success":
#     print("Data quality checks failed. Blocking deployment.")
#     exit(1)

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DataQualityChecks")
    .config("spark.jars", "jars/deequ-2.0.3-spark-3.3.jar,jars/scala-library-2.12.15.jar")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

data = spark.createDataFrame(
    [(1, 10), (2, 20), (3, 30)],
    ["transaction_id", "sales_amount"]
)

check = Check(spark, CheckLevel.Error, "Data quality checks") \
    .hasSize(lambda x: x >= 1) \
    .isComplete("transaction_id") \
    .isNonNegative("sales_amount")

result = VerificationSuite(spark).onData(data).addCheck(check).run()

if result.status != "Success":
    print("Data quality checks failed. Blocking deployment.")
else:
    print("Data quality checks passed.")