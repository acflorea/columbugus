package dr.acf.spark.evaluation

import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.Map

/**
  * Evaluator for multiclass / multilabel classification.
  * Created by aflorea on 12.12.2015.
  *
  * @see
  * A systematic analysis of performance measures for classification tasks
  * Marina Sokolova a, * , Guy Lapalme b
  */
class MulticlassMultilabelMetrics(predictionAndLabels: RDD[(Seq[Double], Double)]) {

  /**
    * An auxiliary constructor taking a DataFrame.
    * @param predictionAndLabels a DataFrame with two double columns: prediction and label
    */
  private def this(predictionAndLabels: DataFrame) =
    this(predictionAndLabels.map(r => (r.getSeq[Double](0), r.getDouble(1))))

  private lazy val labelCountByClass: Map[Double, Long] = predictionAndLabels.values.countByValue()
  private lazy val labelCount: Long = labelCountByClass.values.sum
  private lazy val tpByClass: Map[Double, Int] = predictionAndLabels
    .map { case (predictions, label) =>
      (label, if (predictions.contains(label)) 1 else 0)
    }.reduceByKey(_ + _)
    .collectAsMap()
  private lazy val fpByClass: Map[Double, Int] = predictionAndLabels
    .flatMap { case (predictions, label) =>
      predictions map (prediction => (prediction, if (prediction == label) 0 else 1))
    }.reduceByKey(_ + _)
    .collectAsMap()
  private lazy val confusions = predictionAndLabels
    .flatMap { case (predictions, label) =>
      predictions map (prediction => ((label, prediction), 1))
    }.reduceByKey(_ + _)
    .collectAsMap()

  /**
    * Returns confusion matrix:
    * predicted classes are in columns,
    * they are ordered by class label ascending,
    * as in "labels"
    */
  def confusionMatrix: Matrix = {
    val n = labels.length
    val values = Array.ofDim[Double](n * n)
    var i = 0
    while (i < n) {
      var j = 0
      while (j < n) {
        values(i + j * n) = confusions.getOrElse((labels(i), labels(j)), 0).toDouble
        j += 1
      }
      i += 1
    }
    Matrices.dense(n, n, values)
  }


  /**
    * Returns true positive rate for a given label (category)
    * @param label the label.
    */
  def truePositiveRate(label: Double): Double = recall(label)

  /**
    * Returns false positive rate for a given label (category)
    * @param label the label.
    */
  def falsePositiveRate(label: Double): Double = {
    val fp = fpByClass.getOrElse(label, 0)
    fp.toDouble / (labelCount - labelCountByClass(label))
  }

  /**
    * Returns precision for a given label (category)
    * @param label the label.
    */
  def precision(label: Double): Double = {
    val tp = tpByClass(label)
    val fp = fpByClass.getOrElse(label, 0)
    if (tp + fp == 0) 0 else tp.toDouble / (tp + fp)
  }

  /**
    * Returns recall for a given label (category)
    * @param label the label.
    */
  def recall(label: Double): Double = tpByClass(label).toDouble / labelCountByClass(label)

  /**
    * Returns f-measure for a given label (category)
    * @param label the label.
    * @param beta the beta parameter.
    */
  def fMeasure(label: Double, beta: Double): Double = {
    val p = precision(label)
    val r = recall(label)
    val betaSqrd = beta * beta
    if (p + r == 0) 0 else (1 + betaSqrd) * p * r / (betaSqrd * p + r)
  }

  /**
    * Returns f1-measure for a given label (category)
    * @param label the label.
    */
  def fMeasure(label: Double): Double = fMeasure(label, 1.0)

  /**
    * Returns precision
    */
  lazy val precision: Double = tpByClass.values.sum.toDouble / labelCount

  /**
    * Returns recall
    * (equals to precision for multiclass classifier
    * because sum of all false positives is equal to sum
    * of all false negatives)
    */
  lazy val recall: Double = precision

  /**
    * Returns f-measure
    * (equals to precision and recall because precision equals recall)
    */
  lazy val fMeasure: Double = precision

  /**
    * Returns weighted true positive rate
    * (equals to precision, recall and f-measure)
    */
  lazy val weightedTruePositiveRate: Double = weightedRecall

  /**
    * Returns weighted false positive rate
    */
  lazy val weightedFalsePositiveRate: Double = labelCountByClass.map { case (category, count) =>
    falsePositiveRate(category) * count.toDouble / labelCount
  }.sum

  /**
    * Returns weighted averaged recall
    * (equals to precision, recall and f-measure)
    */
  lazy val weightedRecall: Double = labelCountByClass.map { case (category, count) =>
    recall(category) * count.toDouble / labelCount
  }.sum

  /**
    * Returns weighted averaged precision
    */
  lazy val weightedPrecision: Double = labelCountByClass.map { case (category, count) =>
    precision(category) * count.toDouble / labelCount
  }.sum

  /**
    * Returns weighted averaged f-measure
    * @param beta the beta parameter.
    */
  def weightedFMeasure(beta: Double): Double = labelCountByClass.map { case (category, count) =>
    fMeasure(category, beta) * count.toDouble / labelCount
  }.sum

  /**
    * Returns weighted averaged f1-measure
    */
  lazy val weightedFMeasure: Double = labelCountByClass.map { case (category, count) =>
    fMeasure(category, 1.0) * count.toDouble / labelCount
  }.sum

  /**
    * Returns the sequence of labels in ascending order
    */
  lazy val labels: Array[Double] = tpByClass.keys.toArray.sorted
}