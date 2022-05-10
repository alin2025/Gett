from pyspark.sql import SparkSession
import os
import pyarrow as pa
import json as js
import pyspark.sql.functions as f
from pyspark.sql import types as t
from pyspark.sql.functions import *
from IPython.core.display import display
from dateutil.parser import parse
from pyspark.sql import DataFrameWriter
import pandas as pd
import configuration as c
import mysql.connector as mc
from time import time
from datetime import datetime
#============================================================================================#
#================== integrate wth kafka======================================================#
#============================================================================================#
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell'


#============================================================================================#
#================== connection between  spark and kafka=======================================#
#============================================================================================#
spark = SparkSession.builder\
    .appName("FromKafkaToHdfsArchive") \
    .getOrCreate()

#============================================================================================#
#=========================================== ReadStream from kafka===========================#
#consume the data from Kafka using Spark structured streaming.===============================#
socketDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", c.bootstrapServers) \
    .option("Subscribe", c.topic2)\
    .load()\
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
#============================================================================================#
#==============================Create schema for create df from json=========================#
#============================================================================================#
schema = t.StructType() \
    .add("vendorid", t.StringType())                    .add("lpep_pickup_datetime", t.StringType()) \
    .add("lpep_dropoff_datetime", t.StringType())       .add("store_and_fwd_flag", t.StringType()) \
    .add("ratecodeid", t.StringType())                  .add("pickup_longitude", t.StringType()) \
    .add("pickup_latitude", t.StringType())             .add("dropoff_longitude", t.StringType()) \
    .add("dropoff_latitude", t.StringType())            .add("passenger_count", t.StringType()) \
    .add("trip_distance", t.StringType())               .add("fare_amount", t.StringType()) \
    .add("extra", t.StringType())                       .add("mta_tax", t.StringType()) \
    .add("tip_amount", t.StringType())                  .add("tolls_amount", t.StringType()) \
    .add("improvement_surcharge", t.StringType())       .add("total_amount", t.StringType())\
    .add("payment_type", t.StringType())                .add("trip_type", t.StringType())

#============================================================================================#
#==========================change json to dataframe with schema==============================#
#============================================================================================#
kafka_to_hdfs_json = socketDF\
    .select(f.col("value")
    .cast("string")) \
    .select(f.from_json(f.col("value"), schema)\
    .alias("value")).select("value.*")

#============================================================================================#
#======= Create Date object from Date in String type=========================================#
#============================================================================================#
def stringToDateTime(num):
    if num is not None:
        return parse(num)
    else:
        return None
dateTimeUdf = udf(stringToDateTime, t.TimestampType())

#============================================================================================#
#====2 : Update data types of date and time columns==========================================#
#============================================================================================#

#==========2.1 converte string To Date Time
kafka_to_hdfs_json = kafka_to_hdfs_json\
    .withColumn("pickup_datetime",dateTimeUdf(kafka_to_hdfs_json.lpep_pickup_datetime)) \
    .withColumn("dropoff_datetime", dateTimeUdf(kafka_to_hdfs_json.lpep_dropoff_datetime))

#==========2.2 extract date from timestampe
kafka_to_hdfs_json = kafka_to_hdfs_json\
    .withColumn('TripStartDT', kafka_to_hdfs_json['pickup_datetime'].cast('date'))\
    .withColumn('TripEndDT', kafka_to_hdfs_json['dropoff_datetime'].cast('date'))

#==========2.3 extract year,month,day,dayofweek,week_day_full from date
#==========2.4 extract hour,minute,second from timestampe
# hourd = f.udf(lambda x: x.hour, t.IntegerType())
# minute = f.udf(lambda x: x.minute, t.IntegerType())
# second = f.udf(lambda x: x.second, t.IntegerType())


kafka_to_hdfs_json = kafka_to_hdfs_json\
    .withColumn("TripStartDay", dayofmonth(kafka_to_hdfs_json["TripStartDT"])) \
    .withColumn("TripStartMonth", month(kafka_to_hdfs_json["TripStartDT"]))\
    .withColumn("TripStartYear", year(kafka_to_hdfs_json["TripStartDT"]))

kafka_to_hdfs_json = kafka_to_hdfs_json\
    .withColumn("hour", hour(kafka_to_hdfs_json["pickup_datetime"])) \
    .withColumn("minute", minute(kafka_to_hdfs_json["pickup_datetime"])) \
    .withColumn("second", second(kafka_to_hdfs_json["pickup_datetime"]))\
    .withColumn("dayofweek", dayofweek(kafka_to_hdfs_json["TripStartDT"])) \
    .withColumn("week_day_full", date_format(col("TripStartDT"), "EEEE"))





#Writing to HDFS archive
#In case of a failure or intentional shutdown, you can recover the previous progress and state of a previous query,
# and continue where it left off. This is done using checkpointing, which is a "must" in HDFS writing queries.
kafka_to_hdfs_json.printSchema()
hdfs_query = kafka_to_hdfs_json\
    .writeStream\
    .partitionBy("TripStartYear","TripStartMonth","TripStartDay","hour","minute","second")\
    .format("json") \
    .option("path",c.kafka_to_hdfs_json_path)\
    .option("checkpointLocation",c.kafka_to_hdfs_json_checkpoint_path)\
    .outputMode("append") \
    .start()
hdfs_query.awaitTermination()

#
# Testing
# df = spark.read.json('hdfs://Cnt7-naya-cdh63:8020/user/alin/de_proj/hdfsarchive/TripStartYear=2015')
# df.show(5)
