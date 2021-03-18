import pandas as pd

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf, col, concat_ws
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, to_date

from pyspark import SparkFiles

# set configs from file
config = configparser.ConfigParser()
config.read('configs/global.cfg')

KEY = config.get('AWS', 'AWS_ACCESS_KEY_ID')
SECRET = config.get('AWS','AWS_SECRET_ACCESS_KEY')

input_path = config.get('PATH', 'INPUT_DATA_FOLDER')
output_path = config.get('PATH', 'OUTPUT_DATA_FOLDER')

raw_flight_data_path = input_path + config.get('PATH', 'FLIGHTS_RAW_FOLDER')
raw_tweets_data_path = input_path + config.get('PATH', 'TWEETS_RAW_FOLDER')


# init spark process
def create_spark_session():
    """
    - Create or retrieve existing spark session
    
    Returns: 
        spark -- SparkSession object 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("dfs.client.read.shortcircuit.skip.checksum", "true")\
        .getOrCreate()
    return spark


spark = create_spark_session()
sc = spark.sparkContext

# get flights data

flights_df = spark.read.options( 
            recursiveFileLookup=True , 
            inferSchema=True, 
            header=True)\
        .csv( raw_flight_data_path )

flights_staging = flights_df.selectExpr( "callsign", "icao24 as trasponder_id", 
                      "registration as aircraft_id", "typecode as aircraft_type",
                     "origin as depart_airport_id", "destination as arrival_airport_id",
                        "firstseen as depart_at", "lastseen as arrival_at")\
    .filter("arrival_airport_id is not null")


# get airports data - to enrich existing data
spark.sparkContext.addFile("https://ourairports.com/data/airports.csv")

airports_df = spark.read.csv("file://" +SparkFiles.get("airports.csv"), header=True, inferSchema= True)

airports_staging = airports_df.selectExpr("id", "ident as code", "type", "name", "iso_country", "municipality")


# write parquet files to local (S3 mount in case of EMR)
flights_staging.write.parquet(output_path + "/flights.parquet", mode="overwrite")                
airports_staging.write.parquet(output_path + "/airports.parquet", mode="overwrite")


"""
tweets_df = spark.read.options( 
            recursiveFileLookup=True , 
            inferSchema=True, 
            header=True)\
        .json( raw_tweets_data_path )

tweets_staging = tweets_df.select(['date', 'keywords', 'location.country', 'tweet_id'])\
    .withColumn("keywords", concat_ws(",", col("keywords")))\
                .filter( col("location").isNotNull() )

tweets_staging.write.parquet(output_path + "/tweets.parquet", mode="overwrite")

"""