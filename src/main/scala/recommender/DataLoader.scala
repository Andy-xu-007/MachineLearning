package recommender

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @param product
 * @param name
 * @param imageUrl
 * @param categories
 * @param tags
 */
case class Product(product: Int, name: String, imageUrl: String, categories: String, tags: String)

/**
 * 评分
 *
 * @param userId
 * @param productId
 * @param score
 * @param timestamp
 */
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

/**
 * MongoDB 连接配置
 * @param uri
 * @param db
 */
case class MongoConfig(uri: String, db: String)

object DataLoader {
  val PRODUCT_DATA_PATH = ""
  val RATING_DATA_PATH = ""

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.url" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "remonnender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val productRDD: RDD[String] = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
//    val frame: DataFrame = spark.read.format("libsvm").load(PRODUCT_DATA_PATH)
    productRDD.map(item => {
      val attr = item.split("\\^")
      Product(attr(0).toInt, )
    })



  }
}
