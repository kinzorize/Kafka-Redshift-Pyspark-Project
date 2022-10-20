#import library
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col


# we need redshift jdbc jar file
jar_path = 'redshift-jdbc42-2.0.0.4.jar'


# create spark session object
spark = SparkSession \
    .builder \
    .appName("integration_pyspark_redshift") \
    .config("spark.jars", jar_path)\
    .config('spark.driver.extraClassPath', jar_path) \
    .getOrCreate()

# provide aws credentials
spark._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", "")
spark._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", "")

# create spark datafrme from csv file
AirlineDF = spark.read.option("header", "true").csv(
    "/Users/ghost/Documents/airline/airlines1.csv")

AirlineDF.show()

# create new dataframe and select some columns
AirlineDF1 = AirlineDF.select("Year", "Reporting_Airline")

AirlineDF1.show()

# Writing dataframe into Redshift
AirlineDF1.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cq86xufekc54.us-east-1.redshift.amazonaws.com:5439/test_data?user=awsuser&password=123Success") \
    .option("Tempdir", "s3://airlines-uchechukwu123/airline-storage/") \
    .option("dbtable", "airline") \
    .mode("overwrite") \
    .save()

# Read data from redshift
df = spark.read.format("jdbc") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cq86xufekc54.us-east-1.redshift.amazonaws.com:5439/test_data?user=awsuser&password=123Success") \
    .option("Tempdir", "s3://airlines-uchechukwu123/airline-storage/") \
    .option("dbtable", "airline") \
    .load()

df.show()

# Read data from a Query
df1 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cq86xufekc54.us-east-1.redshift.amazonaws.com:5439/test_data?user=awsuser&password=123Success") \
    .option("query", "select count(*) from airline") \
    .option("Tempdir", "s3://airlines-uchechukwu123/airline-storage/") \
    .load()

df1.show()
