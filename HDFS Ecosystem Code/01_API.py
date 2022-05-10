from kafka import KafkaProducer
import time,json,requests
from datetime import datetime
import  configuration as c

while True:
    # ======== Read from Remote API  ==================== #
    response = requests.get(url=c.green_taxi_events_api)
    # print(response.json())
    for line in range(len(response.json())):
        row = response.json()[line]
        print(row)
        time.sleep(1)
        #==============send to consumer==========================#
        producer = KafkaProducer(bootstrap_servers=c.bootstrapServers)
        producer.send(topic=c.topic2, value=json.dumps(row).encode('utf-8'))    #From_Kafka_To_Hdfs_Archive_Json
        producer.send(topic=c.topic3, value=json.dumps(row).encode('utf-8'))    #From_Kafka_To_Spark_MYSQL
        producer.send(topic=c.topic4, value=json.dumps(row).encode('utf-8'))    #From_Kafka_To_Hdfs_Parquet
    print(datetime.fromtimestamp(time.time()))
    time.sleep(3)








