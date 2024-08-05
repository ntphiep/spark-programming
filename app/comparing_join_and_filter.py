import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Comparing").getOrCreate()

