package dr.acf.recc

import dr.acf.extractors.{BugData, DBExtractor}
import dr.acf.spark._
import dr.acf.spark.SparkOps._
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * The main algorithm behind the Reccomender System
  * Created by aflorea on 18.11.2015.
  */
object ReccomenderBackbone extends SparkOps {

  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._

  val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]) {

    logger.debug("Start!")
    val startTime = System.currentTimeMillis()

    // Execution parameters
    val cleansing = conf.getBoolean("phases.cleansing")
    val transform = conf.getBoolean("phases.transform")
    val preprocess = conf.getBoolean("phases.preprocess")
    val pca = conf.getBoolean("preprocess.pca")
    val chi2 = conf.getBoolean("preprocess.chi2")
    val lda = conf.getBoolean("preprocess.lda")

    // File System root
    val fsRoot = conf.getString("filesystem.root")


    // Step 1 - load data from DB

    val wordsData: DataFrame = dataCleansing(cleansing, fsRoot)

    val rescaledData: DataFrame = dataTransform(transform, fsRoot, wordsData)

    val categoryScalingFactor = conf.getDouble("preprocess.categoryScalingFactor")
    val chi2Features = conf.getInt("preprocess.chi2Features")
    val ldaTopics = conf.getInt("preprocess.ldaTopics")
    val ldaOptimizer = conf.getString("preprocess.ldaOptimizer")

    val (trainingData, testData, trainingCount, testCount) = if (preprocess) {

      // Integrate more features
      val compIds = rescaledData.select("component_id").map(_.getAs[Int](0)).distinct().collect()

      // val assignment = rescaledData.select("assigned_to", "assignment_class").distinct().collect()
      // assignment.foreach(println)

      // Step 2.a one vs all SVM
      val includeCategory = conf.getBoolean("preprocess.includeCategory")
      val normalize = conf.getBoolean("preprocess.normalize")

      val normalizer = new feature.Normalizer()

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
            Array.concat(Array(categoryScalingFactor), features.values)
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
        .map(rowToLabeledPoint).zipWithIndex()

      // Split data into training (90%) and test (10%).
      val allDataCount = rawData.count()
      val trainingCount = allDataCount / 10 * 9 toInt

      val trainingData = rawData.filter(_._2 <= trainingCount).map(_._1).cache()
      val testData = rawData.filter(_._2 > trainingCount).map(_._1).cache()

      /** PCA */
      val (trainingData_PCA, testData_PCA) = if (pca) {
        // Compute the top 10 principal components.
        val PCAModel = new feature.PCA(100).fit(trainingData.map(_.features))
        // Project vectors to the linear space spanned by the top 10 principal components, keeping the label
        (trainingData.map(p => p.copy(features = PCAModel.transform(p.features))),
          testData.map(p => p.copy(features = PCAModel.transform(p.features))))
      }
      else {
        (trainingData, testData)
      }

      /** CHI2 */
      val (trainingData_CHI2, testData_CHI2) = if (chi2) {
        // Create ChiSqSelector that will select top 10000
        val selector = new ChiSqSelector(chi2Features)
        // Create ChiSqSelector model (selecting features)
        val cached = trainingData_PCA.cache()
        val transformer = selector.fit(cached)
        cached.unpersist()
        // Filter the top features from each feature vector
        val filteredTrainingData = trainingData_PCA.map { lp =>
          LabeledPoint(lp.label, transformer.transform(lp.features))
        }
        val filteredTestData = testData_PCA.map { lp =>
          LabeledPoint(lp.label, transformer.transform(lp.features))
        }
        (filteredTrainingData, filteredTestData)
      }
      else {
        (trainingData_PCA, testData_PCA)
      }

      /** LDA */
      val (trainingData_LDA, testData_LDA) = if (lda) {
        // Index documents with unique IDs
        val zippedData = trainingData_CHI2.union(testData_CHI2).zipWithIndex.map(_.swap)
        val corpus = zippedData.map(point => (point._1, point._2.features))
        // Cluster the documents into n topics using LDA
        val ldaModel = new LDA().setK(ldaTopics)
          .setOptimizer(ldaOptimizer)
          .run(corpus.cache())
          .asInstanceOf[DistributedLDAModel]
        corpus.unpersist()
        // Output topics. Each is a distribution over words (matching word count vectors)
        println(s"LDA :: Learned $ldaTopics topics (as distributions over vocab of ${ldaModel.vocabSize} words)")

        val transformed = ldaModel.topicDistributions.join(zippedData).map {
          point =>
            val index = point._1
            val dataPair = point._2
            val topics = dataPair._1
            val labelPoint = dataPair._2
            (index, LabeledPoint(labelPoint.label, topics))
        }

        val transformedTraining = transformed.filter(_._1 <= trainingCount).map(_._2)
        val transformedTest = transformed.filter(_._1 > trainingCount).map(_._2)

        (transformedTraining, transformedTest)
      }
      else {
        (trainingData_CHI2, testData_CHI2)
      }

      trainingData_LDA.saveAsObjectFile(s"$fsRoot/acf_training_data_${chi2Features}_$ldaTopics")
      testData_LDA.saveAsObjectFile(s"$fsRoot/acf_test_data_${chi2Features}_$ldaTopics")

      val _trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_${chi2Features}_$ldaTopics")
      val _testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_${chi2Features}_$ldaTopics")

      (_trainingData, _testData, trainingCount, allDataCount - trainingCount)

    } else {
      val _trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_${chi2Features}_$ldaTopics")
      val _testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_${chi2Features}_$ldaTopics")
      // Split data into training (90%) and test (10%).
      val trainingCount = _trainingData.count()
      val testCount = _testData.count()

      (_trainingData, _testData, trainingCount, testCount)
    }

    logger.debug(s"Training data size $trainingCount")
    logger.debug(s"Test data size $testCount")

    // Run training algorithm to build the model
    val undersample = conf.getBoolean("preprocess.undersampling")
    val model = new SVMWithSGDMulticlass(undersample).train(trainingData, 100, 1, 0.01, 1)

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

    println("categoryScalingFactor = " + categoryScalingFactor)
    println("fMeasure = " + fMeasure)
    println("Weighted Precision = " + weightedPrecision)
    println("Weighted Recall = " + weightedRecall)
    println("Weighted fMeasure = " + weightedFMeasure)
    // println("Confusion Matrix = " + metrics.confusionMatrix.toString(500, 10000))

    // Step 3...Infinity - TDB

    val stopHere = true

    logger.debug(s"We're done in ${(System.currentTimeMillis() - startTime) / 1000} seconds!")
  }

  /**
    * If transform : TF/IDF, FREQUENCY-FILTERING, SAVE DATAFRAME else : LOAD DATAFRAME
    * @param transform
    * @param fsRoot - file system root
    * @param wordsData
    * @return - transformed dataframe
    */
  private def dataTransform(transform: Boolean, fsRoot: String, wordsData: DataFrame): DataFrame = {
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
        .filter(pair => pair._1.length > 2)
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
    rescaledData
  }

  /**
    * If cleansing : LOAD, FILTER, TOKENIZE, SAVE DATAFRAME - else LOAD DATAFRAME
 *
    * @param cleansing
    * @param fsRoot - file system root
    * @return - cleaned dataframe
    */
  private def dataCleansing(cleansing: Boolean, fsRoot: String): DataFrame = {
    val wordsData = if (cleansing) {
      logger.debug("CLEANSING :: Start!")

      // Charge configs
      val tokenizerType = conf.getInt("global.tokenizerType")
      val minWordSize = conf.getInt("global.minWordSize")

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
    wordsData
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
