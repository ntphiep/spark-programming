from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("TextFileProcessing") \
                  .set("spark.executor.memory", "4g") \
                  .set("spark.driver.memory", "4g") \
                  .set("spark.executor.cores", "4")
                  
sc = SparkContext(conf=conf)

my_rdd = sc.textFile("file:///home/hiep/work/spark-k/data/compare/ex1.txt")

print("-----------------", my_rdd.getNumPartitions())

# print(my_rdd.collect())