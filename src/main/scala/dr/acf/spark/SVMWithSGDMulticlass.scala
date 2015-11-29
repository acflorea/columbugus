package dr.acf.spark

import org.apache.spark.mllib.classification.{ClassificationModel, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by aflorea on 29.11.2015.
  */
class SVMWithSGDMulticlass {

  /**
    * Train k (one vs. all) SVM models given an RDD of (label, features) pairs. We run a fixed number
    * of iterations of gradient descent using a step size of 1.0. We use the entire data set to
    * update the gradient in each iteration.
    * NOTE: Labels used in SVM should be {0, 1, 2 ... k-1}
    *
    * @param input RDD of (label, array of features) pairs.
    * @param numIterations Number of iterations of gradient descent to run.
    * @return a SVMModel which has the weights and offset from training.
    */
  def train(input: RDD[LabeledPoint], numIterations: Int): SVMMultiModel = {

    // determine number of classes
    val numberOfClasses = input.map(point => point.label).max().toInt

    val binaryModels = (0 until numberOfClasses).map { i =>

      // one vs all - map class labels
      val inputProjection = input.map {
        case LabeledPoint(label, features) => LabeledPoint(if (label == i) 1.0 else 0.0, features)
      }

      // train each model
      inputProjection.cache()
      val model = SVMWithSGD.train(inputProjection, numIterations)
      inputProjection.unpersist(false)

      model.clearThreshold()
      model

    }.toArray

    new SVMMultiModel(binaryModels)
  }

}

object SVMWithSGDMulticlass {

}

/**
  * A bag of one-vs-all models
  * @param models
  */
class SVMMultiModel(models: Array[SVMModel]) extends ClassificationModel with Serializable {

  val indexedModels = models.zipWithIndex

  override def predict(testData: RDD[Vector]): RDD[Double] = ???

  override def predict(testData: Vector): Double = {
    val binaryPredictions = indexedModels.map(im => (im._1.predict(testData), im._2))
    binaryPredictions
      .maxBy { case (score, index) => score }
      ._2
  }

}