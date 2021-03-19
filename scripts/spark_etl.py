import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, concat_ws
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, to_date

# from pyspark import SparkFiles

# set configs from file
config = configparser.ConfigParser()
config.read('global.cfg')


# init spark process
def create_spark_session():
    """
    - Create or retrieve existing spark session
    
    Returns: 
        spark -- SparkSession object 
    """
    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .config("dfs.client.read.shortcircuit.skip.checksum", "true")\
        .getOrCreate()
    return spark

def main():

    print("Spark ETL: Started")

    spark = create_spark_session()
    sc = spark.sparkContext

    # get flights data

    print("Spark ETL: Flights")


    flights_df = spark.read.options( 
                recursiveFileLookup=True , 
                inferSchema=True, 
                header=True)\
            .csv( "s3a://udacity-awss/flights_raw" )

    flights_staging = flights_df.selectExpr( "callsign", "icao24 as trasponder_id", 
                          "registration as aircraft_id", "typecode as aircraft_type",
                         "origin as depart_airport_id", "destination as arrival_airport_id",
                            "firstseen as depart_at", "lastseen as arrival_at")\
        .filter("arrival_airport_id is not null")

    print("Spark ETL: Airports")

    
    # get airports data - to enrich existing data
    spark.sparkContext.addFile("https://ourairports.com/data/airports.csv")

    airports_df = spark.read.csv("file://" +SparkFiles.get("airports.csv"), header=True, inferSchema= True)

    airports_staging = airports_df.selectExpr("id", "ident as code", "type", "name", "iso_country", "municipality")
    

    print("Spark ETL: Writing Files")

    # write parquet files to local (S3 mount in case of EMR)
    flights_staging.write.parquet("s3a://udacity-awss/output/flights.parquet", mode="overwrite")                
    airports_staging.write.parquet("s3a://udacity-awss/output/airports.parquet", mode="overwrite")

    spark.stop()

    print("Spark ETL: Complete")

if __name__ == "__main__":
    main()


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