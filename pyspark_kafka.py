#Install this package in order to interact with kafka
!pip install kafka-python

from json import dumps
from kafka import KafkaProducer
import pandas as pd
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8')


data = pd.read_csv('/Users/ghost/Documents/Dependencies/airlines1.csv')
data.head()

df = data[["Unnamed: 0","Year", "Reporting_Airline"]]
df.head()

df.rename(columns = {'Unnamed: 0':'id'}, inplace = True)

df.head()

df.shape

dict_data = df.to_dict('records')

dict_data[0]

# Extracting only 1000 rows of the data
for e in range(1000):
    producer.send("airline-topic", value=dict_data[e],key=json.dumps(dict_data[e]["id"]).encode('utf-8'))


from pyspark.sql.functions import *
from pyspark.sql.types import *

#import library 
import os
from pyspark.sql import SparkSession

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

sc = SparkSession.builder.appName('Pyspark_kafka_airline_read_write').getOrCreate()

df = sc \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "airline-topic") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load() \
    .select("value") \
    .selectExpr("CAST(value AS STRING) as json")

df.show()

# create the json datatype
jsonSchema = StructType([StructField("id", StringType(), True), StructField("Year", StringType(), True),
                                     StructField("Reporting_Airline", StringType(), True)])
                            

# Parsing and selecting the right column data
df = df.withColumn("jsonData", from_json(col("json"), jsonSchema)) \
                .select("jsonData.*")


df.show()

# Transformation on data

df.select('Reporting_Airline').groupBy('Reporting_Airline').count()


df_filtered = df.select('id','Year','Reporting_Airline').filter('Year >= 2015')

# Putting data back to kafka

query = df_filtered.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")\
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("topic", "airline-sink") \
                .option("checkpointLocation", "./check") \
                .save()