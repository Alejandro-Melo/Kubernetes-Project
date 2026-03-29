
from pyspark import RDD as rdd
import json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf

def parse_log_to_spark():
    # include Hadoop S3A jars (adjust versions to match your Hadoop)
    spark = SparkSession.builder \
    .appName("LogProcess").getOrCreate()

    # read / write using s3a://
    #logs = spark.read.text("s3a://sparkbucket/Apache.log")
    logs = spark.read.text("files/Apache.log")

    #extract date 
    date_pattern = r'\[([(\w)]+)\s([(\w)]+)\s([(\w)]+)\s(.*?)\s([(\w)]+)\]'
    datemap = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}
    dates = logs.withColumn("Month", F.regexp_extract("value", date_pattern, 2))
    
    @udf
    def dates_mapping(x):
        for value, key in datemap.items():
            if value == x:
                return key
            
    dates = dates.withColumn("Month_final", dates_mapping("Month"))
    parsed = dates.withColumn("date", 
                              F.to_timestamp(
                               F.concat(
                                   F.regexp_extract("value", date_pattern, 3),
                                   F.lit("-"),
                                   dates.Month_final,
                                   F.lit("-"),
                                   F.regexp_extract("value", date_pattern, 5),
                                   F.lit(" "),
                                   F.regexp_extract("value", date_pattern, 4)
                                        ),
                                    'dd-MM-yyyy HH:mm:ss'
                                )
                            )
     # primary groups: date, type, content
    pattern = r'\[(.*?)\]\s\[(.*?)\]\s(?:\[client\s(.*)\]\s)?(.*)'
    parsed = parsed.withColumn("type", F.regexp_extract("value", pattern, 2)) \
                 .withColumn("content", F.regexp_extract("value", pattern, 4)) \
                 .withColumn("client", F.regexp_extract("value", pattern, 3))
    
    # write partitioned, columnar output for efficient downstream processing
    #parsed.select("date", "type", "content", "client").write.mode("overwrite").parquet("s3a://sparkbucket/Apache.parquet")
    parsed.select("date", "type", "content", "client").write.mode("overwrite").parquet("files/Apache.parquet")
if __name__ == "__main__":
    parse_log_to_spark()