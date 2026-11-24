from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import date, datetime
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

print("Showing the first row of the dataframe")
spark_dataframe.show(1)  # Showing 1st row in the dataframe.

print("Showing the first row of the dataframe in vertical format")
spark_dataframe.show(
    1, vertical=True
)  # Showing 1st row in the dataframe in vertical format.

print("Listing all column names in the dataframe")
print(spark_dataframe.columns)  # Listing all column names in the dataframe.

print("Printing the schema of the dataframe")
spark_dataframe.printSchema()  # Printing the schema of the dataframe.

print("Showing summary statistics of selected columns in the dataframe")
spark_dataframe.select(
    "a", "b", "c"
).describe().show()  # Showing summary statistics of the dataframe.

"""
DataFrame.collect() collects the distributed data to the driver side as the local data in Python. 
Note that this can throw an out-of-memory error when the dataset is too large to fit in the 
driver side because it collects all the data from executors to the driver side.
"""

print("Collecting all rows in the dataframe")
print(spark_dataframe.collect())  # Collecting all rows in the dataframe.

print("Taking the first 2 rows of the dataframe")
print(spark_dataframe.take(2))  # Taking the first 2 rows of the dataframe.

print("Converting the Spark DataFrame to a Pandas DataFrame")
print(
    spark_dataframe.toPandas()
)  # Converting the Spark DataFrame to a Pandas DataFrame.
