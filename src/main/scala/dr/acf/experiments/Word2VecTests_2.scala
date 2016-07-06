package dr.acf.experiments

import dr.acf.spark.SparkOps._
import dr.acf.spark.SparkOps
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.feature

/**
  * Created by acflorea on 18/02/16.
  */
object Word2VecTests_2 extends SparkOps {

  def main(args: Array[String]) {

    // File System root
    val fsRoot = conf.getString("filesystem.root")

    val w2vmodel_build = false

    if (w2vmodel_build) {
      val numericalData = sqlContext.read.parquet(s"$fsRoot/acf_numerical_data")

      // Learn a mapping from words to Vectors.
      val word2Vec = new Word2Vec()
        .setInputCol("words")
        .setOutputCol("word2vec")
        .setVectorSize(100)
        .setMinCount(0)

      val model = word2Vec.fit(numericalData)
      val result = model.transform(numericalData)

      // Save model
      model.save(s"$fsRoot/Word2VecModel")
      // Save data2
      result.write.mode("overwrite").parquet(s"$fsRoot/acf_word2vec_data")

    } else {

      val data = sqlContext.read.parquet(s"$fsRoot/acf_word2vec_data")
      val model = Word2VecModel.load(s"$fsRoot/Word2VecModel")

      println("Mumu!")
    }

  }

}
