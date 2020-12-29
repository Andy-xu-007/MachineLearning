from pyspark.sql import SparkSession
from pyspark.ml.feature import Bucketizer

# Bucketizer 按照给定边界离散化
spark = SparkSession\
    .builder\
    .appName("BucketizerDemo")\
    .getOrCreate()

split = [-float("inf"), -0.5, 0.0, 0.5, float("inf")]

data = [(-999.9,), (-0.5,), (-0.3,), (0.0,), (0.2,), (999.9,)]
dataFrame = spark.createDataFrame(data, ["feature"])

bucketizer = Bucketizer(splits=split, inputCol="feature", outputCol="Bucketizer_Feature")

bucketizerDataFrame = bucketizer.transform(dataFrame)

print("Bucketizer output with %d buckets" % (len(bucketizer.getSplits()) -1))

bucketizerDataFrame.show()

spark.stop()


