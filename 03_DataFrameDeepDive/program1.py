from pyspark.sql import SparkSession

# Creating a SparkSession 
spark = SparkSession \
         .builder \
         .appName("First Application") \
         .getOrCreate() 
         

# Create a single column DF 
df1 = spark.range(10)
df1.printSchema() 
df1.show()


