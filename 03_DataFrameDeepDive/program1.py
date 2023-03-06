from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Creating a SparkSession 
spark = SparkSession \
         .builder \
         .appName("First Application") \
         .getOrCreate() 
         

data_set = 's3://fcc-spark-example/dataset/flight-time.json'

schema_ddl = """FL_DATE STRING, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
                ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
                WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED STRING, DISTANCE INT"""
                
                
raw_df = spark.read \
                .format("json") \
                .schema(schema_ddl) \
                .option("mode", "FAILFAST") \
                .option("dateFormat", "M/d/y") \
                .load(data_set)
                

raw_df_2 = raw_df \
            .withColumn("FL_DATE", F.to_date("FL_DATE", "M/d/y")) \
            .withColumn("CANCELLED", F.expr("if(CANCELLED==1, true, false)"))

raw_df_2.show(10)

input("Please wait!!")