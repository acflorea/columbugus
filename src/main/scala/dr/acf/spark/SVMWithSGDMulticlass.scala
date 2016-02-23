package dr.acf.spark

import org.apache.spark.mllib.classification.{ClassificationModel, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization.{L1Updater, HingeGradient}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.parallel.ForkJoinTaskSupport

/**
  * Created by aflorea on 29.11.2015.
  */
class SVMWithSGDMulticlass(undersample: Boolean, seed: Long) {

  def logger = LoggerFactory.getLogger(getClass.getName)
  val resultsLog = LoggerFactory.getLogger("resultsLog")

  /**
    * Train k (one vs. all) SVM models given an RDD of (label, features) pairs. We run a fixed number
    * of iterations of gradient descent using the specified step size. Each iteration uses
    * `miniBatchFraction` fraction of the data to calculate the gradient. The weights used in
    * gradient descent are initialized using the initial weights provided.
    *
    * NOTE: Labels used in SVM should be {0, 1}.
    *
    * @param input             RDD of (label, array of features) pairs.
    * @param numIterations     Number of iterations of gradient descent to run.
    * @param stepSize          Step size to be used for each iteration of gradient descent.
    * @param regParam          Regularization parameter.
    * @param miniBatchFraction Fraction of data to be used per iteration.
    */
  def train(
             input: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double): SVMMultiModel = {

    // determine number of classes
    val numberOfClasses = input.map(point => point.label).distinct().count().toInt

    resultsLog.info(s"Training SVMWithSGDMulticlass for $numberOfClasses distinct classes")

    val binaryModelIds = (0 until numberOfClasses).par

    binaryModelIds.tasksupport =
      new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(10))

    val binaryModels = binaryModelIds.map { i =>

      // one vs all - map class labels
      val inputProjection = input.map {
        case LabeledPoint(label, features) => LabeledPoint(if (label == i) 1.0 else 0.0, features)
      }

      logger.debug(s"Train $i vs all with ${inputProjection filter (_.label == 1.0) count()} positive samples")

      val trainData = if (undersample) {
        val positives = inputProjection filter (_.label == 1.0)
        val positivesCount = positives.count()
        // Perform random undersampling - Handling imbalanced datasets: A review
        // http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.96.9248
        val rawNegatives = inputProjection filter (_.label == 0.0)
        val negativesCount = rawNegatives.count()
        val samplingRate = (Math.max(positivesCount, 100) * 10.0) / negativesCount
        val negatives = if (samplingRate < 1.0)
          rawNegatives.sample(withReplacement = false, samplingRate, seed)
        else
          rawNegatives
        positives union negatives
      }
      else
        inputProjection

      // train each model
      trainData.cache()
      // val svm = new SVMWithSGD()
      // svm.optimizer.setUpdater(new L1Updater)
      val model = SVMWithSGD.train(trainData, numIterations, stepSize, regParam, miniBatchFraction)
      // val model = svm.run(trainData)
      trainData.unpersist(false)

      model.clearThreshold()
      model

    }.toArray

    new SVMMultiModel(binaryModels)

  }

  /**
    * Train k (one vs. all) SVM models given an RDD of (label, features) pairs. We run a fixed number
    * of iterations of gradient descent using a step size of 1.0. We use the entire data set to
    * update the gradient in each iteration.
    * NOTE: Labels used in SVM should be {0, 1, 2 ... k-1}
    *
    * @param input         RDD of (label, array of features) pairs.
    * @param numIterations Number of iterations of gradient descent to run.
    * @return a SVMModel which has the weights and offset from training.
    */
  def train(input: RDD[LabeledPoint], numIterations: Int): SVMMultiModel = {
    train(input, numIterations, 1.0, 0.01, 1.0)
  }

}

object SVMWithSGDMulticlass {

}

/**
  * A bag of one-vs-all models
  *
  * @param models array of one vs. all models
  */
class SVMMultiModel(models: Array[SVMModel])
  extends ClassificationModel with Serializable {

  val indexedModels = models.zipWithIndex

  /**
    * Predict values for the given data set using the model trained.
    *
    * @param testData RDD representing data points to be predicted
    * @return RDD[Double] where each entry contains the corresponding prediction
    *
    */
  override def predict(testData: RDD[Vector]): RDD[Double] = {
    val localModels = models.zipWithIndex
    val bcModels = testData.context.broadcast(localModels)
    testData.mapPartitions { iter =>
      val w = bcModels.value
      iter.map(v => predict(v, w)._2)
    }
  }

  /**
    * Predict values for a single data point using the model trained.
    *
    * @param testData array representing a single data point
    * @return predicted category from the trained model
    */
  override def predict(testData: Vector): Double = predict(testData, indexedModels)._2

  /**
    * Predict 1 value for a single data point using the model trained.
    *
    * @param testData array representing a single data point
    * @return predicted category from the trained model
    */
  def predict1(testData: Vector): (Double, Int) = predict(testData, indexedModels)


  private def predict(testData: Vector, models: Array[(SVMModel, Int)], howMany: Int = 1): (Double, Int) = {
    val binaryPredictions = models.map(im => (im._1.predict(testData), im._2))
    val max = binaryPredictions
      .maxBy { case (score, index) => score }
    max
  }

}