package ML

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

/**
 * Bucketizer 连续型数据处理之给定边界离散化
 *
 */
object Bucketizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BucketizerExample")
      .master("local[*]")
      .getOrCreate()

    val array = Array((1,13.0),(2,16.0),(3,23.0),(4,35.0),(5,56.0),(6,44.0))
    //将数组转为DataFrame
    val df = spark.createDataFrame(array).toDF("id","age")

    // 设定边界，分为5个年龄组：[0,20),[20,30),[30,40),[40,50),[50,正无穷)
    // 注：人的年龄当然不可能正无穷，我只是为了给大家演示正无穷PositiveInfinity的用法，负无穷是NegativeInfinity。
    val splits = Array(0, 20, 30, 40, 50, Double.PositiveInfinity)

    //初始化Bucketizer对象并进行设定：setSplits是设置我们的划分依据
    val bucketizer = new Bucketizer().setSplits(splits).setInputCol("age").setOutputCol("bucketizer_feature")
    //transform方法将DataFrame二值化。
    val bucketizerdf = bucketizer.transform(df)
    //show是用于展示结果
    bucketizerdf.show

    spark.stop()

  }

}
