'''
What does the price discrimination look like for the pickup areas where the taxi drivers make most money, in each 2 hour window?
'''

# Get spark session instance
spark = SparkSession \
    .builder \
    .appName("Kafka Pstr Project 1") \
    .getOrCreate()

lines = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "debs") \
  .load()

# Apply filters
lines = pre_process_df(lines)

# Get specified columns only
columns = [("pickup_datetime", "timestamp"), ("fare_amount", "float"), ("surcharge", "float"), 
          ("mta_tax", "float"), ("tip_amount", "float"), ("tolls_amount", "float"), ("total_amount", "float")]
lines = get_columns(lines, columns)

# Get the cells from locations
lines = get_areas_df(lines)

lines = lines.drop("value","key","topic","partition","offset","timestamp","timestampType","cell_pickup_latitude", "cell_pickup_longitude")

# For each pickup area and each time interval of 2 hours, we compute the sum of the total amount and, of that
# total amount, what percentage corresponds to the fare amount, the surcharge, the mta_tax, the tip_amount 
# and the toll_amount
# We order by the summed total amount, to show these statistics only for the areas where taxi drivers make
# more money
statistics = lines.groupBy("cell_pickup", window("pickup_datetime","2 hours")) \
                  .agg(round(sum("total_amount"),3).alias("sum_total_amount"), \
                       round(sum("fare_amount")/sum("total_amount"),3).alias("fare"), \
                       round(sum("surcharge")/sum("total_amount"),3).alias("surcharge"),\
                       round(sum("mta_tax")/sum("total_amount"),3).alias("mta_tax"),\
                       round(sum("tip_amount")/sum("total_amount"),3).alias("tip"),\
                       round(sum("tolls_amount")/sum("total_amount")).alias("tolls"))\
                  .orderBy("sum_total_amount", ascending=False) \
                  .limit(5)

query = statistics \
            .writeStream \
            .outputMode("complete") \
            .trigger(processingTime="30 seconds") \
            .foreachBatch(dumpBatchDF) \
            .start()

query.awaitTermination(60)

query.stop()
spark.stop()
