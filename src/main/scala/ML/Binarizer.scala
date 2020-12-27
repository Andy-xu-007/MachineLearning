package ML

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{DataFrame, SparkSession}

// Binarizer / 二值化
object Binarizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BinarizerExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val continuousDataFrame = spark.createDataFrame(Seq(
      (0, 1.1),
      (1, 8.5),
      (2, 5.2)
    )).toDF("id", "feature")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(5.1)
    val binarizedDataFrame: DataFrame = binarizer.transform(continuousDataFrame)
    printf("Binarizer output with Threshold = %f", binarizer.getThreshold)
    binarizedDataFrame.show()

    spark.stop()
  }

}
