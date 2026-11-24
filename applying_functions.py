from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import date, datetime
import os, sys
import pandas as pd
from pyspark.sql.functions import pandas_udf

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)


@pandas_udf("long")  # Here pandas udf stands for "Pandas User-Defined Functions."
def pandas_plus_one(series: pd.Series) -> pd.Series:
    return series + 1


def pandas_filter_function(iterator):
    for pandas_df in iterator:
        yield pandas_df[pandas_df.a == 1]


# Creating Spark DataFrame
spark_dataframe = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)

spark_dataframe.select(pandas_plus_one(spark_dataframe.a)).show()

spark_dataframe.mapInPandas(
    func=pandas_filter_function, schema=spark_dataframe.schema
).show()
