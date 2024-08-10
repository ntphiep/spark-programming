import math
from delta import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext



conf = SparkConf().setAppName("prime_checker_10_mil") \
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

                # .set("spark.executor.memory", "2g") \
                # .set("spark.driver.memory", "2g") \
                # .set("spark.driver.maxResultSize", "2g") \
                # .set("spark.local.dir", "/home/hiep/work/spark-k/tmp") \
                # .set("spark.sql.shuffle.partitions", "2") \
                # .set("spark.default.parallelism", "2") \
                # .set("spark.executor.instances", "2") \
                # .set("spark.executor.cores", "2") \
                # .set("spark.task.cpus", "1") \
                # .set("spark.memory.fraction", "0.6") \
                # .set("spark.memory.storageFraction", "0.5") \
                # .set("spark.executor.memoryOverhead", "2g") \
                # .set("spark.driver.memoryOverhead", "2g") \
                # .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                # .set("spark.kryoserializer.buffer.max", "1024m") \
                # .set("spark.kryoserializer.buffer", "512m") \
                # .set("spark.kryo.registrationRequired", "true") \

# builder = SparkSession.builder.appName("prime_checker_10_mil") \
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
# spark = configure_spark_with_delta_pip(builder).getOrCreate()


# sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

def is_prime(n) -> bool:
    if n <= 1: return False
    for i in range(2, math.ceil(math.sqrt(n)) + 1):
        if n % i == 0: return False

    return True

my_rdd = spark.sparkContext.textFile("file:///home/hiep/work/spark-k/data/ex2.txt")

primes = my_rdd.map(lambda x: int(x)) \
        .filter(is_prime) \
        .saveAsTextFile("file:///home/hiep/work/spark-k/result/primes")


print("Number of partitions: ", my_rdd.getNumPartitions())
