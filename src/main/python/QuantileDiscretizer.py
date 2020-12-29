from pyspark.sql import SparkSession
from pyspark.ml.feature import QuantileDiscretizer

# QuantileDiscretizer 按照分位数离散化
spark = SparkSession\
    .builder\
    .appName("QuantileDiscretizer_Demo")\
    .getOrCreate()

data = [(0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2), (5, 9.2), (6, 14.4)]
df = spark.createDataFrame(data, ["id", "hour"])
df = df.repartition(1)  # 数据量小的时候，分区多有可能出错，带核实，本条仅为测试用

# 分成3个桶进行离散化
discretizer = QuantileDiscretizer(numBuckets=3, inputCol="hour", outputCol="result")

result = discretizer.fit(df).transform(df)

result.show()

spark.stop()


