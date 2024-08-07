import sys
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Comparing").master("local[*]").getOrCreate()
sc = spark.sparkContext

rdd_A = sc.parallelize([(1, -1), (2, 20), (3, 3), (4, 0), (5, -12)])
rdd_B = sc.parallelize([(1, 31), (2, 3), (3, 0), (4, -2), (5, 17)])

# rdd_A = spark.sparkContext.textFile("file:///home/hiep/work/spark-k/data/compare/ex1.txt") \
# 		.map(lambda x: (x.split(",")[0], x.split(",")[1]))

# rdd_B = spark.sparkContext.textFile("file:///home/hiep/work/spark-k/data/compare/ex2.txt") \
# 		.map(lambda x: (x.split(",")[0], x.split(",")[1]))

def time_decor(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  
        
        result = func(*args, **kwargs) 
        
        execution_time = time.time()  - start_time  
        print(f"--------Time of '{func.__name__}': {execution_time:.4f} seconds")
        return result  
    return wrapper



@time_decor
def join_first():
    joined_rdd = rdd_A.join(rdd_B)
    filter_rdd = joined_rdd.filter(lambda x: x[1][0] < 1 and x[1][1] >= 6)
    return filter_rdd.collect(), filter_rdd.getNumPartitions()
    
@time_decor
def filter_first():
    rdd_A_f = rdd_A.filter(lambda x: x[1] < 1)
    rdd_B_f = rdd_B.filter(lambda x: x[1] >= 6)
    join_rdd_f = rdd_A_f.join(rdd_B_f)
    return join_rdd_f.collect(), join_rdd_f.getNumPartitions()



match sys.argv[1]:
    case "j":
        print(join_first())
        
    case "f":
        print(filter_first())
        
    case _:
        print("j or f")
        
        
spark.stop()