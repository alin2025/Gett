import os
from pyhive import hive
import pyarrow as pa
import configuration as c



hive_cnx = hive.Connection(host = c.hdfs_host,  port = c.hive_port,username = c.hive_username,password = c.hive_password,auth = c.hive_mode)

#Creating the database
cursor = hive_cnx.cursor()
cursor.execute('''CREATE DATABASE IF NOT EXISTS taxi''')
cursor.close()

#==============================================================================================================================================#
cursor = hive_cnx.cursor()
# # cursor.execute('drop database alin_db')
# # #drop database alin_db
# # #drop table alin_db. traffic_data
cursor.execute('''DROP TABLE IF EXISTS taxi.traffic''')
cursor.close()
#==============================================================================================================================================#
cursor = hive_cnx.cursor()
cursor.execute('SHOW DATABASES')
print(cursor.fetchall())
cursor.close()

#Creating the table============================================================================================================================#
cursor = hive_cnx.cursor()
cursor.execute('''
CREATE TABLE taxi.traffic (
pickup_longitude Double,
pickup_latitude Double,
dropoff_longitude Double,
dropoff_latitude Double,
passenger_count String,
trip_distance Double,
tip_amount Double,
total_amount Double,
TripStartDay int,
TripStartYear int,
hourd int,
minuted int,
secondd int,
dayofweek int,
week_day_full String
                    )
PARTITIONED BY (vendorid String, TripStartMonth int)
stored as parquet''')
cursor.close()
#
cursor = hive_cnx.cursor()
cursor.execute('select * from taxi.traffic')
data = cursor.fetchall()
cursor.close()

#Loading the data========================================
#Listing the partititions========================================
source_path = c.source_path
fs = pa.hdfs.connect(host=c.host,port=c.port,user=c.user,kerb_ticket=None,driver='libhdfs',extra_conf=None)

#print(fs.ls(source_path))
vendorid_list = [f_name.split('=')[-1][0:] for f_name in fs.ls(source_path) if 'vendorid' in f_name]
#print(vendorid_list)

part_list = []
for vendorid in vendorid_list:
    sub_partition = source_path + '/vendorid={}'.format(vendorid)
    for sub in fs.ls(sub_partition):
        TripStartMonth = sub.split('=')[-1][0:]
        part_list.append((vendorid, TripStartMonth))
#print(part_list)

# #Adding partitions-Next we create all the partitions which do not exist yet.
cursor = hive_cnx.cursor()
for vendorid, TripStartMonth in part_list:
    cursor.execute('''ALTER TABLE taxi.traffic
                      ADD IF NOT EXISTS PARTITION (vendorid={}, TripStartMonth={})'''.format(vendorid, TripStartMonth))
cursor.close()

#LOAD the DATA INPATH====================================
cursor = hive_cnx.cursor()
for vendorid, TripStartMonth in part_list:
    try:
        cursor.execute('''LOAD DATA INPATH 'hdfs://Cnt7-naya-cdh63:8020/user/alin/de_proj/traffic_parquet/vendorid={0}/TripStartMonth={1}'
                          INTO TABLE taxi.traffic
                          PARTITION (vendorid={0}, TripStartMonth={1}) '''.format(vendorid, TripStartMonth))
    except:
        pass
cursor.close()

#We can see the actual data by querying the hive table
cursor = hive_cnx.cursor()
cursor.execute('select * from taxi.traffic limit 10')
data = cursor.fetchall()
cursor.close()
len(data)






