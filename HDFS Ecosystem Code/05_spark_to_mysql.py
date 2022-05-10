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
spark = SparkSession.builder.appName("From_Kafka_To_mysql") .getOrCreate()


#==============================================================================================
#=========================================== ReadStream from kafka===========================#
socketDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", c.bootstrapServers) \
    .option("Subscribe", c.topic3)\
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


#===============# Longitude and latitude from the upper left corner of the grid, to help conversion
init_long = -74.916578
init_lat = 41.47718278
# Longitude and latitude from the lower right boundaries for filtering purposes
limit_long = -73.120778
limit_lat = 40.12971598
taxiTripsDF = taxiTripsDF\
    .filter("pickup_longitude> -74.916578 and pickup_longitude< -73.120778 and pickup_latitude>40.12971598 and pickup_latitude<41.47718278 and "
            "dropoff_longitude> -74.916578 and dropoff_longitude< -73.120778 and dropoff_latitude>40.12971598 and dropoff_latitude<41.47718278 and "
            "total_amount>0 and trip_distance>0")

# Longitude and latitude that correspond to a shift in 500 meters
long_shift = 0.005986
lat_shift = 0.004491556

taxiTripsDF = taxiTripsDF \
    .withColumn("cell_pickup_longitude", ceil((taxiTripsDF['pickup_longitude'].cast("double") - init_long) / long_shift)) \
    .withColumn("cell_pickup_latitude", -ceil((taxiTripsDF['pickup_latitude'].cast("double") - init_lat) / lat_shift)) \
    .withColumn("cell_dropoff_longitude", ceil((taxiTripsDF['dropoff_longitude'].cast("double") - init_long) / long_shift)) \
    .withColumn("cell_dropoff_latitude", -ceil((taxiTripsDF['dropoff_latitude'].cast("double") - init_lat) / lat_shift))


taxiTripsDF = taxiTripsDF \
    .withColumn("cell_pickup", concat_ws("-", taxiTripsDF["cell_pickup_latitude"], taxiTripsDF["cell_pickup_longitude"])) \
    .withColumn("cell_dropoff", concat_ws("-", taxiTripsDF["cell_dropoff_latitude"], taxiTripsDF["cell_dropoff_longitude"])) \
    .drop("cell_pickup_latitude", "cell_pickup_longitude", "cell_dropoff_latitude", "cell_dropoff_longitude")

taxiTripsDF = taxiTripsDF.withColumn("route", concat_ws("/", taxiTripsDF["cell_pickup"], taxiTripsDF["cell_dropoff"]))


    #============================================================================================#
#=================================MYSQL============================================#
#============================================================================================##
# Print to check the data values
#taxiTripsDF.show(1, False)
#display(taxiTripsDF.dtypes)
taxiTripsDF.printSchema()
# connector to mysql


def procss_row(events):
    # connector to mysql
    mysql_conn = mc.connect(
        user='naya',
        password='NayaPass1!',
        host='localhost',
        port=3306,
        autocommit=True,  # <--
        database='MyTaxisdb')

    insert_statement = """
    INSERT INTO MyTaxisdb.TAXIs_route(
        vendorid,	            pickup_datetime,	    dropoff_datetime,	store_and_fwd_flag,	ratecodeid,
        pickup_longitude,	    pickup_latitude,	    dropoff_longitude,	dropoff_latitude,	passenger_count,
        trip_distance,	        fare_amount,	        extra,	            mta_tax,	        tip_amount,
        tolls_amount,	        improvement_surcharge,	total_amount,	    PaymentType,	    trip_type,
        TripStartDT,	        TripEndDT,	            TripStartDay,	    TripStartMonth,	    TripStartYear,
        hourd,                  minuted,                secondd,            dayofweek,          week_day_full,
        cell_pickup,            cell_dropoff,           route
)
        VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}',
                '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}',
                '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'); """

    mysql_cursor = mysql_conn.cursor()
    sql = insert_statement.format(
        events["vendorid"],	        events["pickup_datetime"],	    events["dropoff_datetime"],	    events["store_and_fwd_flag"],	events["ratecodeid"],
        events["pickup_longitude"],	events["pickup_latitude"],	    events["dropoff_longitude"],	events["dropoff_latitude"],	    events["passenger_count"],
        events["trip_distance"],	events["fare_amount"],	        events["extra"],	            events["mta_tax"],	            events["tip_amount"],
        events["tolls_amount"],	    events["improvement_surcharge"],events["total_amount"],	        events["PaymentType"],	        events["trip_type"],
        events["TripStartDT"],	    events["TripEndDT"],	        events["TripStartDay"],	        events["TripStartMonth"],	    events["TripStartYear"],
        events["hourd"],            events["minuted"],              events["secondd"],              events["dayofweek"],            events["week_day_full"],
        events["cell_pickup"],      events["cell_dropoff"],         events["route"]
    )
    # print(sql)
    mysql_conn.commit()
    mysql_cursor.execute(sql)
    mysql_cursor.close()
    pass


Insert_To_MYSQL_DB=taxiTripsDF\
    .writeStream\
    .foreach( procss_row)\
    .outputMode("append") \
    .start()
Insert_To_MYSQL_DB.awaitTermination()

