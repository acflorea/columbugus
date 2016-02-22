package dr.acf.experiments

import dr.acf.extractors.DBExtractor._
import dr.acf.spark.SparkOps
import dr.acf.spark.SparkOps._
import org.apache.spark.mllib.feature.{Word2VecModel, Word2Vec}

/**
  * Created by acflorea on 18/02/16.
  */
object Word2VecTests extends SparkOps {

  def main(args: Array[String]) {
    //    val input = sc.textFile("/Users/acflorea/Downloads/text8.txt").map(line => line.split(" ").toSeq)

    val input = mySQLDF(
      s"(select assignment_name from assignments) as assigmnent").map(row => Seq(row.getAs[String](0)))

    val word2vec = new Word2Vec()

    word2vec.setMinCount(1)

    input.take(10).foreach(println)

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("Joe Drew (:JOEDREW!) (off June 18-28)", 40)

    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
    model.save(sc, "myModelPath")
    val sameModel = Word2VecModel.load(sc, "myModelPath")
  }

}
