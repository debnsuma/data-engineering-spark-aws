from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Creating a SparkSession 
spark = SparkSession \
         .builder \
         .appName("First Application") \
         .getOrCreate() 

sc = spark.sparkContext
         

data_set = 's3://fcc-spark-example/dataset/gutenberg_books/1342-0.txt'

rdd1 = sc.textFile(data_set)
rdd2 = rdd1.flatMap(lambda line: line.split(' '))
rdd3 = rdd2.map(lambda word : (word, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)

print(rdd4.take(100))

