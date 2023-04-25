from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Creating a SparkSession 
spark = SparkSession \
         .builder \
         .appName("First Application") \
         .getOrCreate() 