'''
In which routes the total amount per mile is highest?
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
columns = [("pickup_datetime", "timestamp"), ("pickup_longitude", "double"), ("pickup_latitude", "double"),
            ("dropoff_longitude", "double"), ("dropoff_latitude", "double"),
            ("trip_distance", "float"), ("total_amount", "float")]
lines = get_columns(lines, columns)

# Get the cells from locations
lines = get_areas_df(lines)

# Join areas to form routes
lines = get_routes(lines)

lines = lines.drop("value","key","topic","partition","offset","timestamp","timestampType")

# For each hour and each route, we compute the summed total amount and the summed trip distance 
total_per_route = lines.groupBy(window("pickup_datetime", "1 hours"),"route") \
                       .agg(sum("total_amount").alias("sum_total_amount"),
                            sum("trip_distance").alias("total_distance"))
                        
# We add a new column to the dataframe with the division between the summed total amount and the 
# summed trip distance
profit_per_route = total_per_route.withColumn("amount_per_mile", 
                                               total_per_route["sum_total_amount"]/total_per_route["total_distance"]) \
                                  .drop("sum_total_amount","total_distance")

# We order the results by the total amount per mile, the obtain the top 10 routes in which
# a taxi driver can make more money per mile
most_profitable_routes = profit_per_route.orderBy("amount_per_mile", ascending=False) \
                                         .limit(10)

query = most_profitable_routes \
            .writeStream \
            .outputMode("complete") \
            .trigger(processingTime="5 seconds") \
            .foreachBatch(dumpBatchDF) \
            .start()

query.awaitTermination(60)

query.stop()
spark.stop()
