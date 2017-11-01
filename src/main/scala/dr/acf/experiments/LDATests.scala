package dr.acf.experiments

import dr.acf.spark.SparkOps
import dr.acf.spark.SparkOps._
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row

/**
  * LDA Clustering tests
  * Created by aflorea on 18.01.2016.
  */
object LDATests extends SparkOps {


  // Function to transform row into labeled points
  def rowToLabeledPoint = (row: Row) => {
    val component_id = row.getAs[Int]("component_id")
    val features = row.getAs[SparseVector]("features")
    val assignment_class = row.getAs[Double]("assignment_class")
    val labeledPoint =
      LabeledPoint(
        assignment_class,
        features
      )
    labeledPoint
  }

  def main(args: Array[String]) {
    // Load and parse the data
    val rawData = sqlContext.read.parquet("/home/aflorea/data/columbugus/acf_numerical_data").rdd

    val data = rawData.map(rowToLabeledPoint)

    val parsedData = data.map(lp => lp.features)
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(100).setOptimizer("em").run(corpus)
      .asInstanceOf[DistributedLDAModel]
    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix.asInstanceOf[DenseMatrix]

    val transformed = data.map {
      lp =>
        val features = lp.features.toSparse
        val size = features.size
        val indices = features.indices
        val values = features.values
        val valuesMatrix = new SparseMatrix(
          1,
          size,
          (0 to size map { i => if (indices.contains(i)) 1 else 0 }).scan(0)(_ + _).tail.toArray,
          Array.fill[Int](indices.length)(0),
          values,
          false
        )
        LabeledPoint(lp.label, new DenseVector(valuesMatrix.multiply(topics).values))
    }

    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) {
        print(" " + topics(word, topic))
      }
      println()
    }

    // Save and load model.
    ldaModel.save(sc, "myLDAModel")
    val sameModel = DistributedLDAModel.load(sc, "myLDAModel")
  }
}
