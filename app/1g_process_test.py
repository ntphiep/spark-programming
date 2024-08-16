from pyspark.sql import SparkSession
from pyspark import SparkConf

import sys, os

conf = SparkConf() \
        .setAppName("1g_process") \
        .set("spark.executor.cores", "4") \
        .set("spark.executor.memory", "4g") \
        # .set("spark.executor.memory", "4g") \
        # .set("spark.driver.host", "


spark = SparkSession.builder.config(conf=conf).getOrCreate()


file_path = "file:///data/" if os.name != 'nt' else r"file://C:\Users\DangTinh\Desktop\spark-programming\data\\"


df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("samplingRatio", .1) \
        .load(file_path + "songs_with_attributes_and_lyrics.csv")
        

df.printSchema()


print("--------------------------")
print(df.rdd.getNumPartitions())
print(df.count())
print(df.show(10))

