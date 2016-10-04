package dr.acf.recc

import dr.acf.spark.SparkOps
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.slf4j.LoggerFactory
import SparkOps._

/**
  * Created by acflorea on 03/10/2016.
  */
object PerfMetrics extends SparkOps {

  val timeLog = LoggerFactory.getLogger("executionTimeLog")

  def main(args: Array[String]) {

    val startT = System.currentTimeMillis()

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "/Users/acflorea/Bin/spark-1.6.2-bin-hadoop2.6/data/mllib/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100

    (1 to 100) map (_ => SVMWithSGD.train(training, numIterations))
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    val doneT = System.currentTimeMillis()

    timeLog.debug(s"Training took ${doneT - startT} millis")

  }

}
