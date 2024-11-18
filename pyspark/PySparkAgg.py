import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, quarter
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file
url = ATLAS_URI = os.environ.get('ATLAS_URI')

# Define a function to aggregate movie count by year
def aggregate_by_year(df):
    # Add a year column by parsing the timestamp field 'released'
    df = df.withColumn("year", year(col("released")))

    # Aggregate movie count by year, sort and print year and movie count
    df = df.groupBy("year").count().sort("year", ascending=False)

    print("Explain dag:")
    df.explain()

    print("Number of movies released by year:")
    df.show()

# Define a function to aggregate movie count by quarter
def aggregate_by_quarter(df):

    agg_df = df\
        .filter(col("released").isNotNull())\
        .withColumn("year", year(col("released")))\
        .withColumn("quarter", quarter(col("released")))\
        .filter((df.year >= 2000) & (df.year <= 2005))\
        .groupBy("year", "quarter").count()\
        .sort("year", "quarter", ascending=False)

    print("Explain dag:")
    agg_df.explain()

    print("Number of movies released by quarter:")
    agg_df.show()

# Create a Spark session
spark = SparkSession.builder \
    .appName("MongoDBAggregation") \
    .config("spark.mongodb.read.connection.uri", url) \
    .config("spark.mongodb.write.connection.uri", url) \
    .getOrCreate()

# Print the Spark version
print("spark version:", spark.version)

# Print the PySpark version
print("PySpark version:", pyspark.__version__)

# Read data from MongoDB
df = spark.read\
    .format("mongodb")\
    .option("database", "sample_mflix")\
    .option("collection", "movies")\
    .load()

# # Aggregate by year
# aggregate_by_year(df)

# Aggregate by quarter
aggregate_by_quarter(df)

# # Project away all columns except title and year, filter for year 2010, and show the titles
# df = df.select("title", "year").filter(df.year == 2010)
# print("Number of movies released in 2010:", df.count())
# df.select("title").show(5)

