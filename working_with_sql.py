
from pyspark.sql import SparkSession
import os
import sys
import pandas as pd
from pyspark.sql.functions import pandas_udf

# Set up local Python environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# 1. Read the source CSV file
df = spark.read.csv(
    "files/medical_question_bank.csv",
    header=True,
    inferSchema=True
)

# 2. Inspect the DataFrame schema
df.printSchema()

# 3. Create a temporary view for SQL queries
df.createOrReplaceTempView("medical_question_bank")

# 4. Execute SQL query to count records
result_df = spark.sql("""
    SELECT COUNT(*) AS total_records
    FROM medical_question_bank
""")

result_df.show()


# 5. Define a Pandas UDF
@pandas_udf("integer")
def add_one(s: pd.Series) -> pd.Series:
    return s + 1


# 6. Register the Pandas UDF with Spark SQL
spark.udf.register("add_one", add_one)


# 7. Test the UDF with a constant value
spark.sql("""
    SELECT add_one(CAST(10 AS INT)) AS result
""").show()


# 8. Apply the UDF to an actual DataFrame column
result_df = spark.sql("""
    SELECT
        negative_marks,
        add_one(CAST(negative_marks AS INT)) AS updated_marks
    FROM medical_question_bank
""")

result_df.show(10)


# 9. Stop the Spark session
spark.stop()
