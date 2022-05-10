import configuration as c
import mysql.connector as mc
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import types as t
import json,re
from datetime import datetime
import pyarrow as pa
import os

#===========================connector to mysql====================================#
mysql_conn = mc.connect(
    user=c.mysql_username,
    password=c.mysql_password,
    host=c.mysql_host,
    port=c.mysql_port,
    autocommit=True,
    database=c.mysql_database_name  )

#=======================# Creating the events table==============================#
mysql_cursor_taxi = mysql_conn.cursor()
mysql_create_tbl_events = '''create table if not exists TAXIs_route (
                            vendorid varchar (4) ,
                            pickup_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
                            dropoff_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
                            store_and_fwd_flag varchar (4) ,
                            ratecodeid varchar (4) ,
                            pickup_longitude double ,
                            pickup_latitude double ,
                            dropoff_longitude double ,
                            dropoff_latitude double ,
                            passenger_count varchar (4) ,
                            trip_distance double ,
                            fare_amount double ,
                            extra varchar (4) ,
                            mta_tax double ,
                            tip_amount double ,
                            tolls_amount double ,
                            improvement_surcharge varchar (4) ,
                            total_amount double ,
                            PaymentType varchar (4) ,
                            trip_type varchar (4) ,
                            TripStartDT date ,
                            TripEndDT date ,
                            TripStartDay integer ,
                            TripStartMonth integer ,
                            TripStartYear integer,
                            hourd integer,
                            minuted integer,
                            secondd integer,           
                            dayofweek integer,
                            week_day_full varchar (50),
                            cell_pickup varchar (50),
                            cell_dropoff varchar (50),
                            route varchar (50) )'''

mysql_cursor_taxi.execute(mysql_create_tbl_events)

#
mysql_cursor2 = mysql_conn.cursor()
mysql_cursor2.execute('describe MyTaxisdb.TAXIs_route')
show databases;
use MyTaxisdb;
describe MyTaxisdb.TAXIs_route;
show tables;


mysql_conn = mc.connect(
    user=c.mysql_username,
    password=c.mysql_password,
    host=c.mysql_host,
    port=c.mysql_port,
    autocommit=True,
    database=c.mysql_database_name  )
mysql_cursor_taxi = mysql_conn.cursor()


trip_distance  = mysql_conn.cursor()
trip_distance .execute('SELECT sum(trip_distance  ) FROM MyTaxisdb.TAXIs_green;')
trip_distance  = trip_distance .fetchmany(1)


passenger_count = mysql_conn.cursor()
passenger_count.execute('SELECT sum(passenger_count ) FROM MyTaxisdb.TAXIs_green;')
passenger_count = passenger_count.fetchmany(1)
#fetchone()>>>>method returns a single record or None if no more rows are available.
#cursor.fetchmany(size) >>>>>>returns the number of rows specified by size argument.

print("number of passengers: ",passenger_count)
print("sum of trip_distance: ",trip_distance)
