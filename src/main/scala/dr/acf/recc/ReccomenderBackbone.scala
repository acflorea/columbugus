package dr.acf.recc

import com.typesafe.config.ConfigFactory
import dr.acf.extractors.{BugData, DBExtractor}
import dr.acf.spark._
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * The main algorithm behind the Reccomender System
  * Created by aflorea on 18.11.2015.
  */
object ReccomenderBackbone extends SparkOps {

  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._

  // cofig stuff
  val conf = ConfigFactory.load()

  val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]) {

    logger.debug("Start!")
    val startTime = System.currentTimeMillis()

    // Execution parameters
    val cleansing = conf.getBoolean("phases.cleansing")
    val transform = conf.getBoolean("phases.transform")
    val training = conf.getBoolean("phases.training")
    val testing = conf.getBoolean("phases.testing")
    val pca = conf.getBoolean("phases.pca")
    val chi2 = conf.getBoolean("phases.globalchi2")

    // File System root
    val fsRoot = conf.getString("filesystem.root")

    // Charge configs
    val tokenizerType = conf.getInt("global.tokenizerType")
    val minWordSize = conf.getInt("global.minWordSize")

    // Step 1 - load data from DB

    val wordsData = if (cleansing) {
      logger.debug("CLEANSING :: Start!")
      val currentTime = System.currentTimeMillis()

      val bugInfoRDD: RDD[BugData] = DBExtractor.buildBugsRDD

      // We only care about the "valid" users (#validUsersFilter)
      val bugInfoDF = bugInfoRDD.filter(bugData => bugData.assignment_class >= 0).toDF()

      // Step 2 - extract features
      val tokenizer = tokenizerType match {
        case 0 => new Tokenizer().setInputCol("bug_data").setOutputCol("words")
        case 1 => new RegexTokenizer("\\w+|\\$[\\d\\.]+|\\S+").
          setMinTokenLength(minWordSize).setInputCol("bug_data").setOutputCol("words")
        case 2 => new NLPTokenizer().setInputCol("bug_data").setOutputCol("words")
        case 3 => new POSTokenizer().setInputCol("bug_data").setOutputCol("words")
      }

      val _wordsData = tokenizer.transform(bugInfoDF)

      // writeToTable(_wordsData, "acf_cleaned_data")
      _wordsData.write.mode("overwrite").parquet(s"$fsRoot/acf_cleaned_data")
      logger.debug(s"CLEANSING :: " +
        s"Done in ${(System.currentTimeMillis() - currentTime) / 1000} seconds!")
      sqlContext.read.parquet(s"$fsRoot/acf_cleaned_data")
    }
    else {
      logger.debug("CLEANSING :: Skip!")
      sqlContext.read.parquet(s"$fsRoot/acf_cleaned_data")
    }

    val rescaledData = if (transform) {
      logger.debug("TRANSFORM :: Start!")

      val minDocFreq = conf.getInt("transform.minDocFreq")
      val maxDocFreq = conf.getInt("transform.maxDocFreq")

      val currentTime = System.currentTimeMillis()

      val vocabulary = wordsData.map(r => r.getAs[mutable.WrappedArray[String]]("words"))
        .flatMap(words => words map (word => (word, 1)))
        .reduceByKey(_ + _)

      val invertedIndex = vocabulary.map(pair => (pair._2, 1))
        .reduceByKey(_ + _)

      // Words that appear over a certain threshold
      // thanks case head fix method error line eclipse
      // problem code test project file reply bug attachment patch
      val stopWords = vocabulary
        .filter(pair => pair._2 > maxDocFreq || pair._2 < minDocFreq)
        //.filter(pair => pair._1.length > 2)
        .map(pair => pair._1).collect()

      val stopWordsRemover = new StopWordsRemover().setInputCol("words").setOutputCol("filteredwords")
        .setStopWords(stopWords)
        //.setStopWords(Array.empty[String])

      val vocabularySize = vocabulary.count() - stopWords.length
      logger.debug(s"Vocabulary size $vocabularySize")

      val hashingTF = new HashingTF().setInputCol("filteredwords").setOutputCol("rawFeatures")
        .setNumFeatures(vocabularySize.toInt)

      val featurizedData = hashingTF.transform(stopWordsRemover.transform(wordsData)).cache()

      val idf = new IDF().setMinDocFreq(minDocFreq).setInputCol("rawFeatures").setOutputCol("features")
      val idfModel = idf.fit(featurizedData)

      val _rescaledData = idfModel.transform(featurizedData)

      _rescaledData.write.mode("overwrite").parquet(s"$fsRoot/acf_numerical_data")
      logger.debug(s"TRANSFORM :: " +
        s"Done in ${(System.currentTimeMillis() - currentTime) / 1000} seconds!")
      sqlContext.read.parquet(s"$fsRoot/acf_numerical_data")

    } else {
      logger.debug("CLEANSING :: Skip!")
      sqlContext.read.parquet(s"$fsRoot/acf_numerical_data")
    }

    // Integrate more features
    val compIds = rescaledData.select("component_id").map(_.getAs[Int](0)).distinct().collect()

    // val assignment = rescaledData.select("assigned_to", "assignment_class").distinct().collect()
    // assignment.foreach(println)

    // Step 2.a one vs all SVM
    val categoryScalingFactor = conf.getDouble("training.categoryScalingFactor")
    val includeCategory = conf.getBoolean("training.includeCategory")
    val normalize = conf.getBoolean("training.normalize")

    val normalizer = new feature.Normalizer()

    Seq(categoryScalingFactor) foreach {
      // Seq(82,84,86,88,92,94,96) foreach {

      hyperParam =>

        // Function to transform row into labeled points
        def rowToLabeledPoint = (row: Row) => {
          val component_id = row.getAs[Int]("component_id")
          val features = row.getAs[SparseVector]("features")
          val assignment_class = row.getAs[Double]("assignment_class")

          val labeledPoint = if (includeCategory) LabeledPoint(
            assignment_class,
            Vectors.sparse(
              compIds.length + features.size,
              Array.concat(
                Array(compIds.indexOf(component_id)),
                features.indices map (i => i + compIds.length)
              ),
              Array.concat(Array(hyperParam), features.values)
            )
          )
          else
            LabeledPoint(
              assignment_class,
              features
            )
          if (normalize) {
            labeledPoint.copy(features = normalizer.transform(labeledPoint.features))
          } else {
            labeledPoint
          }
        }

        val rawData = rescaledData.select("index", "component_id", "features", "assignment_class")
          .map(rowToLabeledPoint)

        /** PCA */
        val rawData_PCA = if (pca) {
          // Compute the top 10 principal components.
          val PCAModel = new feature.PCA(100).fit(rawData.map(_.features))
          // Project vectors to the linear space spanned by the top 10 principal components, keeping the label
          rawData.map(p => p.copy(features = PCAModel.transform(p.features)))
        }
        else {
          rawData
        }

        /** CHI2 */
        val rawData_CHI2 = if (chi2) {
          // Discretize data in 16 equal bins since ChiSqSelector requires categorical features
          // Even though features are doubles, the ChiSqSelector treats each unique value as a category
          val discretizedData = rawData_PCA
          //            .map { lp =>
//            LabeledPoint(lp.label, Vectors.dense(lp.features.toArray.map { x => (x * 100 / 16).floor }))
//          }
          // Create ChiSqSelector that will select top 50 of 692 features
          val selector = new ChiSqSelector(10000)
          // Create ChiSqSelector model (selecting features)
          val transformer = selector.fit(discretizedData)
          discretizedData.unpersist()
          // Filter the top 50 features from each feature vector
          val filteredData = discretizedData.map { lp =>
            LabeledPoint(lp.label, transformer.transform(lp.features))
          }
          filteredData
        }
        else {
          rawData_PCA
        }

        val allData = rawData_CHI2
          //.filter(p => p.label < 20.0)
          .zipWithIndex()

        // Split data into training (90%) and test (10%).
        val trainingCount = allData.count() / 10 * 9 toInt

        val trainingData = allData.filter(_._2 <= trainingCount).map(_._1).cache()
        // keep most recent 10% of data for testing
        val testData = allData.filter(_._2 > trainingCount).map(_._1)

        logger.debug(s"Training data size ${trainingData.count()}")
        logger.debug(s"Test data size ${testData.count()}")

        // Run training algorithm to build the model
        val model = new SVMWithSGDMulticlass().train(trainingData, 100, 1, 0.01, 1)

        // Compute raw scores on the test set.
        val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
          val prediction = model.predict(features)
          (prediction, label)
        }

        // Get evaluation metrics.
        val metrics = new MulticlassMetrics(predictionAndLabels)

        val fMeasure = metrics.fMeasure
        val weightedPrecision = metrics.weightedPrecision
        val weightedRecall = metrics.weightedRecall
        val weightedFMeasure = metrics.weightedFMeasure

        println("categoryScalingFactor = " + hyperParam)
        println("fMeasure = " + fMeasure)
        println("Weighted Precision = " + weightedPrecision)
        println("Weighted Recall = " + weightedRecall)
        println("Weighted fMeasure = " + weightedFMeasure)
      // println("Confusion Matrix = " + metrics.confusionMatrix.toString(500, 10000))

    }


    // Step 3...Infinity - TDB

    val stopHere = true

    logger.debug(s"We're done in ${(System.currentTimeMillis() - startTime) / 1000} seconds!")
  }

  def severityLevel(severity: String): Double = {
    severity match {
      case "blocker" => 6.0
      case "critical" => 5.0
      case "enhancement" => 4.0
      case "major" => 3.0
      case "minor" => 2.0
      case "normal" => 1.0
      case "trivial" => 0.0
    }
  }

}
