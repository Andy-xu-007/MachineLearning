package ML

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession

/**
 *    StopWordsRemover  去停用词器
 * 停用词stop words是在文档中频繁出现的词，但未携带太多意义的词，他们不应该参与到算法的运算中
 * StopWordsRemover是将输入的字符串（一般是分词器的Tokenizer的输出）中的停用词删除
 * 停用词表由stopWords参数指定，对于某些语言的默认停用词表是通过调用StopWordsRemover.loadDefaultStopWords(language),
 *  可用的选项为"丹麦" ,“荷兰诧”、“英诧”、“芬兰诧”,“法国”,“德国”、“匈牙利”、“意大利”、“挪威”、“葡萄牙” 、“俄罗斯”、“西班牙”、“瑞典"和"土耳其”
 * 布尔型参数caseSensitive指示是否区分大小写，默认为否
 */
object StopWordsRemover {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("StopWordsRemoverExample")
      .master("local[*]")
      .getOrCreate()

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id","row")

    //stopwordsRemover
    val remover = new StopWordsRemover()
      .setInputCol("row")
      .setOutputCol("filtered")

    remover.transform(dataSet).show(false)
    spark.stop()
  }

}
