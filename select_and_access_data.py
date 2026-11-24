from pyspark.sql import SparkSession
from pyspark.sql import Row, Column
from datetime import date, datetime
from pyspark.sql.functions import upper
import os, sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

# Creating Spark DataFrame
spark_dataframe = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)


print("Selecting column 'a' from the dataframe")
print(
    spark_dataframe.a
)  # Selecting a column does not trigger computation but it returns a Column instance.


# Testing expressions on columns.
print("Testing expressions on columns.")
result: bool = (
    type(spark_dataframe.c)
    == type(upper(spark_dataframe.c))
    == type(spark_dataframe.c.isNull())
)
print(result)

print("Selecting a column")
spark_dataframe.select(
    spark_dataframe.c
).show()  # Selecting column 'c' from the dataframe.

print("Adding a new column")
spark_dataframe.withColumn(
    "upper_c", upper(spark_dataframe.c)
).show()  # Adding a new column 'upper_c' with uppercase values of column 'c'.

print("Filtering rows where column 'a' equals 1")
spark_dataframe.filter(
    spark_dataframe.a == 1
).show()  # Applying a filter to select rows where column 'a' equals 1.
