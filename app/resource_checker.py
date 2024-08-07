from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("resource_checker").getOrCreate()


sc = spark._jsc.sc() 
n_workers = len([executor.host() for executor in sc.statusTracker().getExecutorInfos() ]) - 1

print("Number of workers: ", n_workers)

###################
sc = spark._jsc.sc() 

result1 = sc.getExecutorMemoryStatus().keys() # will print all the executors + driver available

result2 = len([executor.host() for executor in sc.statusTracker().getExecutorInfos() ]) -1

print(result1, end ='\n')
print(result2)