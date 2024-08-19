from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf() \
        .setAppName("Comparing") \
        
        
spark = SparkSession.builder.config(conf=conf).getOrCreate()

sc = spark._jsc.sc()

n_workers = len([executor.host() for executor in sc.statusTracker().getExecutorInfos() ]) - 1

sc.sc().getExecutorMemoryStatus().keys() # will print all the executors + driver available

