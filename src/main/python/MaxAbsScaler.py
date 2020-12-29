from pyspark.sql import SparkSession
from pyspark.ml.feature import MaxAbsScaler
from pyspark.ml.linalg import Vectors

# MaxAbsScaler 最大值最小值幅度缩放
spark = SparkSession \
    .builder \
    .appName("MaxAbsScaler_Demo") \
    .getOrCreate()

dataFrame = spark.createDataFrame([
    (0, Vectors.dense([1.0, 0.1, -8.0]),),  # Vectors.dense 稠密向量
    (1, Vectors.dense([2.0, 1.0, -4.0]),),
    (2, Vectors.dense([4.0, 10.0, 8.0]),)],
    ["id", "features"])

scaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")

# 计算最大最小值用于缩放
scalerModel = scaler.fit(dataFrame)

# 缩放幅度到[-1, 1]之间
scaledData = scalerModel.transform(dataFrame)
scaledData.select("features", "scaledFeatures").show()

spark.stop()
