#to run spark-submit spark.py
#Serenitylesa4411!

from pyspark.sql import SparkSession  
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
# from pyspark.sql.functions import *
# from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
import pyspark.sql.functions as F


scSpark = SparkSession \
    .builder \
    .appName("Uk Traffic Accidents 2012-2014") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sdfData = scSpark.read.csv("accidents_2012_to_2014.csv", header=True, inferSchema = True)

# print(sdfData.schema)
# sdfData.orderBy("Longitude").show()
# sdfData.select("Longitude").show()

#Must make sure you purge whitespace or match exactly on column names
# print(sdfData.groupBy().sum('Age').collect())

df_stats = sdfData.select(
    F.mean(F.col('Longitude')).alias('mean'),
    F.stddev(F.col('Longitude')).alias('std')
).collect()

mean = df_stats[0]['mean']
std = df_stats[0]['std']

print "mean of longitude = %f" %(mean)

print "standard deviation of longitude = %f" %(std)

df_stats = sdfData.select(
    F.mean(F.col('Latitude')).alias('mean'),
    F.stddev(F.col('Latitude')).alias('std')
).collect()

mean = df_stats[0]['mean']
std = df_stats[0]['std']

print "mean of latitude = %f" %(mean)

print "standard deviation of latitude = %f" %(std)


answer = sdfData.groupBy("Day_of_Week").count().orderBy("count", ascending=False).show()

answer = sdfData.groupBy("Road_Type").count().orderBy("count", ascending=False).show()

answer = sdfData.groupBy("Time").count().orderBy("count", ascending=False).show()

answer = sdfData.groupBy("Number_of_Casualties").count().orderBy("count", ascending=False).show()

answer = sdfData.groupBy("Junction_Control").count().orderBy("count", ascending=False).show()

answer = sdfData.groupBy("Speed_limit").count().orderBy("count", ascending=False).show()

# df.where((col("foo") > 0) & (col("bar") < 0))

# answer = sdfData.where((F.col("Accident_Severity") < 2 & (F.col("Number_of_Vehicles") < 2))).count().show()

# answer = sdfData.groupBy("Accident_Severity").where((F.col("Accident_Severity") < 2 & (F.col("Number_of_Vehicles") < 2))).count().show()

# tdata.withColumn("Age",  when((tdata.Age == "" & tdata.Survived == "0"), mean_age_0).otherwise(tdata.Age)).show()

