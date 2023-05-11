from pyspark.sql import SparkSession

# Creating a Spark Session instance
spark = (
    SparkSession
        .builder
        .appName("Word Count for all the books")
        .getOrCreate() 
)

# Creating a Spark Context 
sc = spark.sparkContext

# Data set 
data_set = 's3://fcc-spark-example/dataset/gutenberg_books/*'

# Data processing (All are transformation)
result = (sc.textFile(data_set) 
            .flatMap(lambda line: line.split(' ')) 
            .map(lambda word: (word, 1)) 
            .reduceByKey(lambda x, y: x + y)
         )

# Save the processed data (final output)
result_folder = 's3://fcc-spark-example/output/word_count_all_books_output'
result.saveAsTextFile(result_folder) # This is an Action 






