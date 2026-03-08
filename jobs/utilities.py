import re
import polars as pl
from pyspark.sql import SparkSession, functions as F

def parse_log_to_spark():
    spark = SparkSession.builder \
    .appName("LogParser") \
    .getOrCreate()

    # read raw lines in parallel
    logs = spark.read.text("../files/Apache.log")

    # primary groups: date, type, content
    pattern = r'(\[.*?\])\s(\[.*?\])\s(.*)'
    parsed = logs.withColumn("date", F.regexp_extract("value", pattern, 1)) \
                 .withColumn("type", F.regexp_extract("value", pattern, 2)) \
                 .withColumn("content", F.regexp_extract("value", pattern, 3))

    # extract client IP if present
    client_pattern = r'client\ (\d[\d\.]*)'
    parsed = parsed.withColumn("client", F.regexp_extract("value", client_pattern, 1))

    # write partitioned, columnar output for efficient downstream processing
    parsed.select("date", "type", "content", "client") \
          .write.mode("overwrite").parquet("../files/Apache.parquet")

if __name__ == "__main__":
    parse_log_to_spark()