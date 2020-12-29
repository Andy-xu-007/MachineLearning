from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler

# StandardScaler 标准化
spark = SparkSession \
    .builder \
    .appName("StandardScaler_Demo") \
    .getOrCreate()

spark.read.format("libsvm").load("")
