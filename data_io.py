from pyspark.sql import SparkSession
import os, sys

# Set up local environment paths
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# 1. Read the source file
df = spark.read.csv("files/medical_question_bank.csv", header=True, inferSchema=True)
df.show(10)

# 2. Select specific columns and limit to 10 rows (keeps it as a DataFrame)
df_subset = df.select([
    "code",
    "stem",
    "question_type",
    "difficulty_level",
    "marks",
    "negative_marks"
]).limit(10)
    
# 3. Write directly using PySpark's native writer (overwrites if it already exists)
df_subset.write.mode("overwrite").csv("files/medical_question_bank_subset.csv", header=True)
