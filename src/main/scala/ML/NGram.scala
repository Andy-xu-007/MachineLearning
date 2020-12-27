package ML

import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *    N-Gram N元模型
 * 一个n-gram是一个长度为n的字的序列。 N-Gram的输入为一系列的字符串，例如Tokenizer分词器的输出。
 *    参数n表示每个n-gram中单词terms的数量，输出由n-gram序列组成，
 *    其中每个n-gram由空格分隔的n个连续词的字符串表示 如果输入的字符串序列少于n个单词，NGram输出为空
 */
object NGram {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("NGramExample")
      .master("local[*]")
      .getOrCreate()

    val wordDataFrame: DataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))))
      .toDF("id", "words")

    //ngram model
    val nGram: NGram = new NGram()
      .setN(2)
      .setInputCol("words")
      .setOutputCol("ngrams")

    // transfrom
    val nGramDF = nGram.transform(wordDataFrame)

    nGramDF.select("ngrams").show(false)

    spark.stop()

  }

}
