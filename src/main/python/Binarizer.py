from pyspark.sql import SparkSession
from pyspark.ml.feature import Binarizer

## Binarizer 二值化
spark = SparkSession\
    .builder\
    .appName("BinarizerDemo")\
    .getOrCreate()

continuousDataFrame = spark.createDataFrame([
    (0, 1.1),
    (1, 8.5),
    (2, 5.2)],
    ["id", "feature"])

binarizer = Binarizer(threshold=5.1, inputCol="feature", outputCol="Binarizer_feature")

binarizerDataFrame = binarizer.transform(continuousDataFrame)

print("Binarizer output with Threshold = %f" % binarizer.getThreshold())

binarizerDataFrame.show()

spark.stop()
