from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import Row
from datetime import datetime, date
import os, sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.getOrCreate()

# Enable eager evaluation for Spark SQL in the REPL
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

# Creating Spark DataFrame
spark_dataframe1 = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)

print(spark_dataframe1)

spark_dataframe2 = spark.createDataFrame(
    [
        (1, 2.0, "string1", date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
        (2, 3.0, "string2", date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
        (4, 5.0, "string3", date(2000, 3, 1), datetime(2000, 1, 3, 12, 0)),
    ],
    schema="a long, b double, c string, d date, e timestamp",
)

print(spark_dataframe2)


# Creating Spark DataFrame from Pandas DataFrame.

pandas_dataframe = pd.DataFrame(
    {
        "a": [1, 2, 3],
        "b": [2.0, 3.0, 4.0],
        "c": ["string1", "string2", "string3"],
        "d": [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
        "e": [
            datetime(2000, 1, 1, 12, 0),
            datetime(2000, 1, 2, 12, 0),
            datetime(2000, 1, 3, 12, 0),
        ],
    }
)

spark_dataframe_from_pandas = spark.createDataFrame(pandas_dataframe)
print(spark_dataframe_from_pandas)

print("Data frame 1")
spark_dataframe1.show()

print("Data frame 2")
spark_dataframe2.show()

print("Data frame 2 with 1 row")
spark_dataframe2.show(1)
spark_dataframe2.show(1, vertical=True)

print("Data frame from pandas")
spark_dataframe_from_pandas.printSchema()  # Shows the summary of the dataframe.
