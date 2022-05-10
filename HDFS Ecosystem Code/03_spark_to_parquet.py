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

#================== integrate wth kafka======================================================#
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell'

#================== connection between  spark and kafka=======================================#
#==============================================================================================
spark = SparkSession.builder.appName("From_Kafka_To_parquet") .getOrCreate()


#==============================================================================================
#=========================================== ReadStream from kafka===========================#
socketDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", c.bootstrapServers) \
    .option("Subscribe", c.topic4)\
    .load()\
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
#==============================================================================================
#==============================Create schema for create df from json=========================#
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

#==============================================================================================
#==========================change json to dataframe with schema==============================#
taxiTripsDF = socketDF.select(f.col("value").cast("string")).select(f.from_json(f.col("value"), schema).alias("value")).select("value.*")

#====# 1: Remove spaces from column names====================================================#
taxiTripsDF = taxiTripsDF \
    .withColumnRenamed("vendorid", "vendorid")                          .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")     .withColumnRenamed("passenger_count", "passenger_count") \
    .withColumnRenamed("trip_distance", "trip_distance")                .withColumnRenamed("ratecodeid", "ratecodeid") \
    .withColumnRenamed("store_and_fwd_flag", "store_and_fwd_flag")      .withColumnRenamed("payment_type", "PaymentType")

#============================================================================================#
#====Cleanse money column (convert to  float by removing '$' from the data) =================#
def normalizeMoney(num):
    if num is not None:
        return float(num.replace('$', ''))
    else:
        return 0.0
#============================================================================================#
#======= Create Date object from Date in String type=========================================#
#============================================================================================#
def stringToDateTime(num):
    if num is not None:
        return parse(num)
    else:
        return None
dateTimeUdf = udf(stringToDateTime, t.TimestampType())
moneyUdf = udf(normalizeMoney, t.DoubleType())
#============================================================================================#
#====2 : Update data types of money and date time columns====================================#
#============================================================================================#
taxiTripsDF = taxiTripsDF\
    .withColumn("tip_amount", moneyUdf(taxiTripsDF.tip_amount))\
    .withColumn("fare_amount", moneyUdf(taxiTripsDF.fare_amount)) \
    .withColumn("tolls_amount", moneyUdf(taxiTripsDF.tolls_amount))\
    .withColumn("mta_tax", moneyUdf(taxiTripsDF.mta_tax)) \
    .withColumn("total_amount", moneyUdf(taxiTripsDF.total_amount))\
    .withColumn("pickup_datetime",dateTimeUdf(taxiTripsDF.pickup_datetime)) \
    .withColumn("dropoff_datetime", dateTimeUdf(taxiTripsDF.dropoff_datetime))
#============================================================================================#
#==== 3: Add date columns from timestamp=====================================================#
#============================================================================================#
taxiTripsDF = taxiTripsDF.withColumn('TripStartDT', taxiTripsDF['pickup_datetime'].cast('date'))
taxiTripsDF = taxiTripsDF.withColumn('TripEndDT', taxiTripsDF['dropoff_datetime'].cast('date'))

#============================================================================================#
#==== 4: Add/convert/casting Additional columns types=========================================#
#============================================================================================#
taxiTripsDF = taxiTripsDF\
    .withColumn('trip_distance', taxiTripsDF['trip_distance'].cast('double'))\
    .withColumn('pickup_longitude', taxiTripsDF['pickup_longitude'].cast('double')) \
    .withColumn('pickup_latitude', taxiTripsDF['pickup_latitude'].cast('double')) \
    .withColumn('dropoff_longitude', taxiTripsDF['dropoff_longitude'].cast('double')) \
    .withColumn('dropoff_latitude', taxiTripsDF['dropoff_latitude'].cast('double'))

#============================================================================================#
#==== 5: Extract Day, Month and Year from timestamp column===================================#
#============================================================================================#
hourd = f.udf(lambda x: x.hourd, t.IntegerType())
minuted = f.udf(lambda x: x.minuted, t.IntegerType())
secondd = f.udf(lambda x: x.secondd, t.IntegerType())


taxiTripsDF = taxiTripsDF\
    .withColumn("TripStartDay", dayofmonth(taxiTripsDF["TripStartDT"])) \
    .withColumn("TripStartMonth", month(taxiTripsDF["TripStartDT"]))\
    .withColumn("TripStartYear", year(taxiTripsDF["TripStartDT"]))

taxiTripsDF = taxiTripsDF\
    .withColumn("hourd", hour(taxiTripsDF["pickup_datetime"])) \
    .withColumn("minuted", minute(taxiTripsDF["pickup_datetime"])) \
    .withColumn("secondd", second(taxiTripsDF["pickup_datetime"]))\
    .withColumn("dayofweek", dayofweek(taxiTripsDF["TripStartDT"])) \
    .withColumn("week_day_full", date_format(col("TripStartDT"), "EEEE"))

## CREATE A CLEANED DATA-FRAME BY DROPPING SOME UN-NECESSARY COLUMNS & FILTERING FOR UNDESIRED VALUES OR OUTLIERS
taxiTripsDF = taxiTripsDF\
    .drop('store_and_fwd_flag','fare_amount','extra','tolls_amount','mta_tax','improvement_surcharge','trip_type','ratecodeid','pickup_datetime','dropoff_datetime','PaymentType','TripStartDT','TripEndDT')\
    .filter("passenger_count > 0 and passenger_count < 8  AND tip_amount >= 0 AND tip_amount < 30 AND fare_amount >= 1 AND fare_amount < 150 AND trip_distance > 0 AND trip_distance < 100 ")


# Print to check the data values
#taxiTripsDF.show(1, False)
#display(taxiTripsDF.dtypes)
taxiTripsDF.printSchema()
# connector to mysql


hdfs_query = taxiTripsDF\
    .writeStream\
    .format("parquet")\
    .partitionBy("vendorid","TripStartMonth")\
    .option("path",c.From_Kafka_To_Hdfs_Parquet_path)\
    .option("checkpointLocation",c.From_Kafka_To_Hdfs_parquet_path_checkpointLocation)\
    .outputMode("append")\
    .start()

hdfs_query.awaitTermination()
