from scipy.spatial import ConvexHull, convex_hull_plot_2d
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("convex_hull").getOrCreate()

df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("file:///home/hiep/work/spark-k/data/points.csv")

df.printSchema()
