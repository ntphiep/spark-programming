from delta import *
import findspark

findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


# conf = SparkConf().setAppName("delta_table_test") \
#                 .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#                 .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


# spark = SparkSession.builder.config(conf=conf).getOrCreate()


# data = spark.read.format("text").load(r"C:\Users\DangTinh\Desktop\spark-programming\data\compare\ex1.txt")
# data.write.format("delta").mode("overwrite").save(r"C:\Userscls\DangTinh\Desktop\spark-programming\data\delta")

builder = SparkSession.builder.appName("delta_table_test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = spark.read.format("text").load(r"C:\Users\DangTinh\Desktop\spark-programming\data\compare\ex1.txt")
data.write.format("delta").mode("overwrite").save(r"C:\Userscls\DangTinh\Desktop\spark-programming\data\delta")