package dr.acf.experiments

import dr.acf.spark.SparkOps._
import dr.acf.spark.SparkOps
import org.apache.spark.ml.feature.{StandardScaler, Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by acflorea on 18/02/16.
  */
object Word2VecTests_2 extends SparkOps {

  def main(args: Array[String]) {

    // File System root
    val fsRoot = conf.getString("filesystem.root")

    val w2vmodel_build = false
    val numFeatures = 100
    val normalize = true

    if (w2vmodel_build) {
      val numericalData = sqlContext.read.parquet(s"$fsRoot/acf_numerical_data")

      // Learn a mapping from words to Vectors.
      val word2Vec = new Word2Vec()
        .setInputCol("words")
        .setOutputCol("word2vec")
        .setVectorSize(numFeatures)
        .setMinCount(2)

      val model = word2Vec.fit(numericalData)
      val result = model.transform(numericalData)

      // Save model
      model.save(s"$fsRoot/Word2VecModel_$numFeatures")
      // Save data2
      result.write.mode("overwrite").parquet(s"$fsRoot/acf_word2vec_data_$numFeatures")

    } else {

      val data = sqlContext.read.parquet(s"$fsRoot/acf_word2vec_data_$numFeatures")
      val model = Word2VecModel.load(s"$fsRoot/Word2VecModel_$numFeatures")

      // Split data into training (90%) and test (10%).
      val allDataCount = data.count()
      val trainingDataCount = allDataCount / 10 * 9 toInt

      // Integrate more features
      val compIds = data.select("component_id").map(_.getAs[Int](0)).distinct().collect()
      val cIds = sc.broadcast(compIds)

      val rescaledData = if (normalize) {
        val scaler = new StandardScaler().setInputCol("word2vec").setOutputCol("word2vec-scaled")
        val scalerModel = scaler.fit(data)
        scalerModel.transform(data).drop("word2vec").withColumnRenamed("word2vec-scaled", "word2vec")
      } else {
        data
      }

      val testData = rescaledData.filter(s"index > $trainingDataCount").map { row =>
        datasetToLabeledPoint(row.getAs[Int]("component_id"), row.getAs[linalg.Vector]("word2vec"), row.getAs[Double]("assignment_class"), cIds.value)
        // LabeledPoint(row.getAs[Double]("assignment_class"), row.getAs[linalg.Vector]("word2vec"))
      }
      val trainingData = rescaledData.filter(s"index <= $trainingDataCount").map { row =>
        datasetToLabeledPoint(row.getAs[Int]("component_id"), row.getAs[linalg.Vector]("word2vec"), row.getAs[Double]("assignment_class"), cIds.value)
        // LabeledPoint(row.getAs[Double]("assignment_class"), row.getAs[linalg.Vector]("word2vec"))
      }

      MLUtils.saveAsLibSVMFile(trainingData.repartition(1), s"$fsRoot/svmInputTrain_W2V$numFeatures")
      MLUtils.saveAsLibSVMFile(testData.repartition(1), s"$fsRoot/svmInputTest_W2V$numFeatures")

    }

  }


  def datasetToLabeledPoint = (component_id: Int, features: Vector, assignment_class: Double, compIds: Array[Int]) => {

    val useCategorical = true

    val labeledPoint = {

      val (_categoryScalingFactor, _categoryMultiplier) = (1, 1)

      features match {
        case sparse: SparseVector =>

          val categoryIndex = Array(compIds.indexOf(component_id).toDouble)
          val categorySize = categoryIndex.length
          val categoryIndices = categoryIndex.map(_ => 0)

          LabeledPoint(
            assignment_class,
            Vectors.sparse(
              categorySize + features.size,
              Array.concat(
                categoryIndices,
                sparse.indices map (i => i + categorySize)
              ),
              Array.concat(categoryIndex, sparse.values)
            )
          )

        case dense: DenseVector =>

          val categoryIndex = Array(compIds.indexOf(component_id).toDouble)

          LabeledPoint(
            assignment_class,
            Vectors.dense(Array.concat(categoryIndex, dense.values))
          )

      }

    }

    labeledPoint
  }


}
