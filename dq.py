from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataQualityChecks").getOrCreate()
data = spark.read.csv("customers-100.csv", header=True, inferSchema=True)
check = Check(spark, CheckLevel.Error, "Data quality checks").hasSize(lambda x: x >= 10000).isComplete("transaction_id").isNonNegative("sales_amount")

result = VerificationSuite(spark).onData(data).addCheck(check).run()

if result.status != "Success":
    print("Data quality checks failed. Blocking deployment.")

exit(1)