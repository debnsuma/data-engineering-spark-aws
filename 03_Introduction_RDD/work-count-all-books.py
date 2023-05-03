from pyspark.sql import SparkSession

# Creating a SparkSession 
spark = SparkSession \
         .builder \
         .appName("First Application") \
         .getOrCreate() 

sc = spark.sparkContext
         

data_set = 's3://fcc-spark-example/dataset/gutenberg_books/*'

rdd1 = sc.textFile(data_set)
rdd2 = rdd1.flatMap(lambda line: line.split(' '))
rdd3 = rdd2.map(lambda word : (word, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)

result_folder = 's3://fcc-spark-example/output/word_count_all_books_output'
rdd4.saveAsTextFile(result_folder)

print(rdd4.take(10))

