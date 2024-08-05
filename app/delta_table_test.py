from delta import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName("delta_table_test") \
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


spark = SparkSession.builder.config(conf=conf).getOrCreate()


data = spark.read.format("text").load("file:///home/hiep/work/spark-k/data/ex2.txt")
data.write.format("delta").mode("overwrite").save("file:///home/hiep/work/spark-k/result/delta_table/")
