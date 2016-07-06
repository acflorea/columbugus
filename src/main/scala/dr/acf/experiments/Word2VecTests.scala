package dr.acf.experiments

import dr.acf.extractors.DBExtractor._
import dr.acf.spark.{NLPTokenizer, POSTokenizer, SparkOps}
import dr.acf.spark.SparkOps._
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

/**
  * Created by acflorea on 18/02/16.
  */
object Word2VecTests extends SparkOps {

  def main(args: Array[String]) {
    //    val input = sc.textFile("/Users/acflorea/Downloads/text8.txt").map(line => line.split(" ").toSeq)


    //    val tokenizer = new RegexTokenizer("\\w+|\\$[\\d\\.]+|\\S+").
    //      setMinTokenLength(2).setInputCol("short_desc").setOutputCol("words")

    // val tokenizer = new NLPTokenizer().setInputCol("short_desc").setOutputCol("words")

    val tokenizer = new POSTokenizer().setInputCol("short_desc").setOutputCol("words")

    val raw_input = mySQLDF(
      s"(select short_desc from bugs union select thetext from longdescs) as vocabulary")

    val input = tokenizer.transform(raw_input).map(row => row.getAs[Seq[String]]("words"))

    val word2vec = new Word2Vec()

    word2vec.setMinCount(1)

    input.take(10).foreach(println)

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("breakpoints", 40)

    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }


    model.transform("breakpoints")

    // Save and load model
    model.save(sc, "myModelPath_2")

  }

}
