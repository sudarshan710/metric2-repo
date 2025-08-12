from pathlib import Path
import os

# 1. Set SPARK_VERSION before importing pydeequ
os.environ["SPARK_VERSION"] = "3.3"

# 2. Prepare JAR file URIs with pathlib (correct file:/// format)
deequ_jar_path = Path(r"C:\Users\sudarshan.zunja\Desktop\dE-tr\day22-cs\jars\deequ-2.0.5-spark-3.3.jar")
scala_jar_path = Path(r"C:\Users\sudarshan.zunja\Desktop\dE-tr\day22-cs\jars\scala-library-2.12.15.jar")

deequ_jar_uri = deequ_jar_path.as_uri()
scala_jar_uri = scala_jar_path.as_uri()

print("Deequ JAR URI:", deequ_jar_uri)
print("Scala lib JAR URI:", scala_jar_uri)

# 3. Set PYSPARK_SUBMIT_ARGS env var BEFORE importing pyspark or pydeequ
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {deequ_jar_uri},{scala_jar_uri} pyspark-shell"

# 4. Now import Spark and pydeequ modules
from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

# 5. Create SparkSession
spark = SparkSession.builder.appName("DeequTest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 6. Sample data
data = spark.createDataFrame([
    (1, 100.0),
    (2, 200.0),
    (3, 300.0)
], ["transaction_id", "sales_amount"])

# 7. Define checks
check = Check(spark, CheckLevel.Error, "Data quality checks") \
    .hasSize(lambda x: x >= 1) \
    .isComplete("transaction_id") \
    .isNonNegative("sales_amount")

# 8. Run verification
result = VerificationSuite(spark).onData(data).addCheck(check).run()

# 9. Interpret results
if result.status != "Success":
    print("❌ Data quality checks failed. Blocking deployment.")
    exit(1)
else:
    print("✅ Data quality checks passed.")

# 10. Stop SparkSession
spark.stop()