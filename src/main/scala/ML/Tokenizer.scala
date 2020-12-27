package ML

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

/**
 * Tokenizer（分词器）
 *
 * Tokenizer是将文本如一个句子拆分成单词的过程，
 * 在spark ml中提供Tokenizer实现此功能
 * RegexTokenizer提供了更高级的基于正则表达式匹配的单词拆分。
 *  默认情况下，参数pattern(默认的正则表达式："\s+") 作为分隔符用于拆分输入的文本，
 *  或者，用户将参数 gaps设置为false，指定正则表达式pattern表示为tokens，而不是分隔符，这样作为划分结果找到的所有匹配项，很简单，主要是看自己业务数据切分的逻辑
 */
object Tokenizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TokenizerExample")
      .master("local[*]")
      .getOrCreate()

    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id","sentence")

    sentenceDataFrame.show(false)

    //Tokenizer实例
    val tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")

    //RegexTokenizer分词器
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W")

    //或者通过gaps设置为false，指定正则表达式pattern表示tokens 而不是分隔符
    val regexTokenizer2 = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\w+")
      .setGaps(false)

    //udf 计算长度
    val countTokens = udf { (words:Seq[String]) => words.length}
    //tokenizer分词结果
    val tokenized: DataFrame = tokenizer.transform(sentenceDataFrame)
    tokenized.select("sentence","words")
      .withColumn("tokens",countTokens(col("words"))).show(false)

    //regexTokenizer分词结果
    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("sentence","words")
      .withColumn("tokens",countTokens(col("words"))).show(false)

    //regexTokenizer分词结果
    val regexTokenized2 = regexTokenizer2.transform(sentenceDataFrame)
    regexTokenized2.select("sentence","words")
      .withColumn("tokens",countTokens(col("words"))).show(false)

    spark.stop()

  }
}
