'''
Detect "slow" areas:

Compute the average idle time of taxis for each area:
The idle time of a taxi is the time mediating between the drop off of a ride, and the pickup time of the following ride.
It is assumed that a taxi is available if it had at least one ride in the last hour.
'''

# Input: (taxi, (pickup area, dropoff area, pickup datetime, dropoff datetime))
# If the time between a dropoff and the next pickup is longer than 1 hour, the taxi is not available (is_available = 0)
def updateFunctionAvailableTaxis(newValues, runningObj):
    if runningObj is None:
        runningObj = (newValues[0][0],newValues[0][1],newValues[0][2],newValues[0][3],0)
    for v in newValues:
        is_available = 1
        if datetime.strptime(runningObj[3], "%Y-%m-%d %H:%M:%S")-datetime.strptime(v[2], "%Y-%m-%d %H:%M:%S") > timedelta(hours=1):
            is_available = 0
        runningObj = (v[0], v[1], v[2], v[3], is_available)
    return runningObj
# Output: (taxi, (pickup area, dropoff area, pickup datetime, dropoff datetime, is_available))

# Input: (taxi, (pickup area, dropoff area, pickup datetime, dropoff datetime))
# For each ride, append the dropoff area and the dropoff datetime of the last ride
def updateFunction(newValues, runningObj):
    if runningObj is None:
        runningObj = (newValues[0][0],newValues[0][1],newValues[0][2],newValues[0][3],0,0)
    else:
        for v in newValues:
            runningObj = (v[0], v[1], v[2], v[3], runningObj[1], runningObj[3])
    return runningObj
# Output: (taxi, (pickup area, dropoff area, pickup datetime, dropoff datetime, old dropoff area, old dropoff datetime))

# Creating the spark context and streaming context objects
sc = SparkContext("local[2]", "KafkaExample")
ssc = StreamingContext(sc, 5)
ssc.checkpoint('checkpoint')
lines = KafkaUtils.createDirectStream(ssc, ["debs"], \
            {"metadata.broker.list": "kafka:9092"})

try:
    # Kafka sends a timestamp followed by the actual value of the row, so keep the second value in the tuple
    filtered_lines = lines.map(lambda line: line[1])
    # Apply filters
    filtered_lines = filtered_lines.filter(lambda line: apply_filters(line))
    # Get areas
    lines_areas = filtered_lines.map(lambda line: get_areas(line))
    
    # We map the needed values to show them in the shape:
    # (taxi, (pickup area, dropoff area, pickup datetime, dropoff datetime))
    datetime_per_taxi = lines_areas.map(lambda line: (line[1],(line[17],line[18],line[2],line[3])))
    
    # Get only the available taxis - We obtain tuples in the shape:
    # (taxi, (pickup area, dropoff area, pickup datetime, dropoff datetime, is_available))
    # And filter them to keep only those with the parameter is_available = 1
    available_taxis = datetime_per_taxi.updateStateByKey(updateFunctionAvailableTaxis) \
                                       .filter(lambda a: a[1][4] == 1) \
                                       .map(lambda a: (a[0],(a[1][0], a[1][1], a[1][2], a[1][3])))
    
    # For each ride, append the dropoff area and the dropoff datetime of the last ride
    # We get the tuples:
    # (taxi, (pickup area, dropoff area, pickup datetime, dropoff datetime, old dropoff area, old dropoff datetime))
    # We filter out the tuples in which the old dropoff time is 0
    extended_rides = available_taxis.updateStateByKey(updateFunction) \
                                    .filter(lambda a: a[1][4] != 0)
    
    # (taxi, (pickup area, dropoff area, pickup datetime, dropoff datetime, old dropoff area, old dropoff datetime))
    # New pickup area must be the same as old dropoff area
    # Old dropoff datetime must be < than new pickup datetime
    processed_extended_rides = extended_rides.filter(lambda a: a[1][0] == a[1][4]) \
                                             .filter(lambda a: datetime.strptime(a[1][5], "%Y-%m-%d %H:%M:%S").timestamp() < datetime.strptime(a[1][2], "%Y-%m-%d %H:%M:%S").timestamp())
    
    # Now we only need the new pickup area, the new pickup datetime and the old dropoff datetime:
    # (pickup area, pickup datetime, old dropoff datetime)
    # We can then compute the idle time for each ride in each area
    idle_time_per_ride_and_area = processed_extended_rides.map(lambda a: (a[1][0], (a[1][2],a[1][5]))) \
                                             .map(lambda a: (a[0], datetime.strptime(a[1][0], "%Y-%m-%d %H:%M:%S")-datetime.strptime(a[1][1], "%Y-%m-%d %H:%M:%S"))) \
                                             .map(lambda a: (a[0], int(a[1].seconds))) \
                                             .transform(lambda rdd: rdd.sortBy(lambda a: a[1], ascending=False))
    
    # (pickup area, idle time)
    # With the idle time for every ride in each area, we can compute the average idle time for each area
    avg_idle_time_per_area = idle_time_per_ride_and_area.map(lambda a: (a[0],(a[1],1))) \
                                                        .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
                                                        .map(lambda a: (a[0], a[1][0]/a[1][1])) \
                                                        .transform(lambda rdd: rdd.sortBy(lambda a: a[1], ascending=False))

    
    avg_idle_time_per_area.pprint()

    ssc.start()
    ssc.awaitTermination(60)
    ssc.stop()
    sc.stop()

except Exception as e:
    print(e)
    ssc.stop()
    sc.stop()
