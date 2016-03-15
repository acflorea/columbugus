package dr.acf.recc

import java.sql.Timestamp

import com.typesafe.config.ConfigRenderOptions
import dr.acf.extractors.{BugData, DBExtractor}
import dr.acf.spark.SparkOps._
import dr.acf.spark._
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, functions}
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
  val resultsLog = LoggerFactory.getLogger("resultsLog")

  def main(args: Array[String]) {

    logger.debug("Start!")

    resultsLog.info(s"NEW RUN AT :${System.currentTimeMillis}")
    resultsLog.info(conf.root().render(ConfigRenderOptions.concise()))

    val startTime = System.currentTimeMillis()

    // Execution parameters
    val cleansing = conf.getBoolean("phases.cleansing")
    val transform = conf.getBoolean("phases.transform")
    val preprocess = conf.getBoolean("phases.preprocess")
    val simple = conf.getBoolean("preprocess.simple")
    val pca = conf.getBoolean("preprocess.pca")
    val chi2 = conf.getBoolean("preprocess.chi2")
    val lda = conf.getBoolean("preprocess.lda")

    // File System root
    val fsRoot = conf.getString("filesystem.root")

    // Step 1 - load data from DB
    val wordsData: DataFrame = dataCleansing(cleansing, fsRoot)

    // Step 2 - transform to numerical features
    val scaledData: DataFrame = dataTransform(transform, fsRoot, wordsData)

    val includeCategory = conf.getBoolean("preprocess.includeCategory")
    val includeProduct = conf.getBoolean("preprocess.includeProduct")

    val categoryScalingFactor = conf.getDouble("preprocess.categoryScalingFactor")
    val categoryMultiplier = conf.getInt("preprocess.categoryMultiplier")
    val productScalingFactor = conf.getDouble("preprocess.productScalingFactor")
    val productMultiplier = conf.getInt("preprocess.productMultiplier")

    val chi2Features = conf.getInt("preprocess.chi2Features")
    val ldaTopics = conf.getString("preprocess.ldaTopics").split(",").map(_.trim.toInt)
    val ldaOptimizer = conf.getString("preprocess.ldaOptimizer")

    val inputDataSVM = mutable.Map.empty[String, (RDD[LabeledPoint], RDD[LabeledPoint])]

    if (preprocess) {

      val normalize = conf.getBoolean("preprocess.normalize")

      // Integrate more features
      val compIds = if (includeCategory) scaledData.select("component_id").map(_.getAs[Int](0)).distinct().collect() else Array.empty[Int]
      val prodIds = if (includeProduct) scaledData.select("product_id").map(_.getAs[Int](0)).distinct().collect() else Array.empty[Int]

      def datasetToLabeledPoint = (featureContext: FeatureContext, component_id: Int, product_id: Int, features: SparseVector, assignment_class: Double) => {
        val labeledPoint = if (includeCategory || includeProduct) {

          val productSize = prodIds.length * productMultiplier
          val productIndices = if (includeProduct)
            1 to productMultiplier map (i => prodIds.length * (i - 1) + prodIds.indexOf(product_id))
          else
            Vector.empty
          val productScalingArray = if (includeProduct)
            1 to productMultiplier map (i => productScalingFactor)
          else
            Vector.empty

          val categorySize = compIds.length * categoryMultiplier
          val categoryIndices = 1 to categoryMultiplier map (i => productSize + compIds.length * (i - 1) + compIds.indexOf(component_id))
          val categoryScalingArray = 1 to categoryMultiplier map (i => categoryScalingFactor)

          LabeledPoint(
            assignment_class,
            Vectors.sparse(
              productSize + categorySize + features.size,
              Array.concat(
                productIndices.toArray,
                categoryIndices.toArray,
                features.indices map (i => i + compIds.length)
              ),
              Array.concat(productScalingArray.toArray, categoryScalingArray.toArray, features.values)
            )
          )
        }
        else
          LabeledPoint(
            assignment_class,
            features
          )
        labeledPoint
      }


      // Function to transform row into labeled points
      def rowToLabeledPoint = (featureContext: FeatureContext, row: Row) => {
        val component_id = row.getAs[Int]("component_id")
        val product_id = row.getAs[Int]("product_id")
        val features = row.getAs[Vector]("features").toSparse
        val assignment_class = row.getAs[Double]("assignment_class")

        datasetToLabeledPoint(featureContext, component_id, product_id, features, assignment_class)
      }

      val minMaxScaling = conf.getBoolean("preprocess.minMaxScaling")
      val _scaledData = if (minMaxScaling) {
        val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
        // Compute summary statistics and generate MinMaxScalerModel
        val scalerModel = scaler.fit(scaledData)
        // rescale each feature to range [min, max].
        scalerModel.transform(scaledData).drop("features").withColumnRenamed("scaledFeatures", "features")
      } else {
        scaledData
      }

      val rescaledData = if (normalize) {
        val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures")
        val scalerModel = scaler.fit(_scaledData)
        scalerModel.transform(_scaledData).drop("features").withColumnRenamed("scaledFeatures", "features")
      } else {
        _scaledData
      }

      // Split data into training (90%) and test (10%).
      val allDataCount = rescaledData.count()
      val trainingDataCount = allDataCount / 10 * 9 toInt

      val rawData = rescaledData.select("index", "component_id", "product_id", "features", "assignment_class")
      //  .map(identity).zipWithIndex().cache()
      //.filter(_._2 < 100)

      resultsLog.info(s"Training data size $trainingDataCount")
      resultsLog.info(s"Test data size ${allDataCount - trainingDataCount}")

      val rawTestData = rescaledData.filter(s"index > $trainingDataCount")
      val rawTrainingData = rescaledData.filter(s"index <= $trainingDataCount")

      // Simple model
      if (simple) {
        logger.debug("Training simple model")

        val trainingData = rawTrainingData.map(rowToLabeledPoint(FeatureContext(Map.empty[String, (Int, Int)]), _))
        val testData = rawTestData.map(rowToLabeledPoint(FeatureContext(Map.empty[String, (Int, Int)]), _))

        trainingData.saveAsObjectFile(s"$fsRoot/acf_training_data_simple")
        testData.saveAsObjectFile(s"$fsRoot/acf_test_data_simple")

        inputDataSVM.put("simple", (trainingData, testData))
      }

      // PCA
      if (pca) {
        logger.debug("Training PCA model")

        /** PCA */
        // Compute the top 100 principal components.
        val pca = new PCA()
          .setInputCol("features")
          .setOutputCol("pcaFeatures")
          .setK(100)
          .fit(rawTrainingData)

        // Project vectors to the linear space spanned by the top 10 principal components, keeping the label
        val trainingData = pca.transform(rawTrainingData).drop("features")
          .withColumnRenamed("pcaFeatures", "features").map(rowToLabeledPoint(FeatureContext(Map.empty[String, (Int, Int)]), _))
        val testData = pca.transform(rawTestData).drop("features")
          .withColumnRenamed("pcaFeatures", "features").map(rowToLabeledPoint(FeatureContext(Map.empty[String, (Int, Int)]), _))

        trainingData.saveAsObjectFile(s"$fsRoot/acf_training_data_PCA_100")
        testData.saveAsObjectFile(s"$fsRoot/acf_test_data_PCA_100")

        inputDataSVM.put("PCA", (trainingData, testData))
      }

      // CHI2
      if (chi2) {
        logger.debug("Training CHI2 model")

        // Create ChiSqSelector that will select top chi2Features features
        val selector = new ChiSqSelector(chi2Features)
        // Create ChiSqSelector model (selecting features)
        val cached = rawTrainingData.select("assignment_class", "features")
          .map { case Row(label: Double, v: Vector) => LabeledPoint(label, v) }.cache()
        val transformer = sc.broadcast(selector.fit(cached))
        cached.unpersist()

        val tfFunction: ((Vector) => Vector) = (document: Vector) => {
          val chi2transformer = transformer.value
          chi2transformer.transform(document)
        }
        val udf_tfFunction = functions.udf(tfFunction)

        // Filter the top features from each feature vector
        val testData = rawTestData.withColumn("CHIFeatures", udf_tfFunction(rawTestData.col("features")))
          .drop("features").withColumnRenamed("CHIFeatures", "features").map(rowToLabeledPoint(FeatureContext(Map.empty[String, (Int, Int)]), _))

        val trainingData = rawTrainingData.withColumn("CHIFeatures", udf_tfFunction(rawTrainingData.col("features")))
          .drop("features").withColumnRenamed("CHIFeatures", "features").map(rowToLabeledPoint(FeatureContext(Map.empty[String, (Int, Int)]), _))


        trainingData.saveAsObjectFile(s"$fsRoot/acf_training_data_CHI2_$chi2Features")
        testData.saveAsObjectFile(s"$fsRoot/acf_test_data_CHI2_$chi2Features")

        inputDataSVM.put("CHI2", (trainingData, testData))
      }

      // LDA
      if (lda) {

        ldaTopics map { ldaTopic =>

          logger.debug("Training LDA model")

          val alpha = conf.getDouble("preprocess.ldaAlpha")
          val beta = conf.getDouble("preprocess.ldaBeta")

          // Index documents with unique IDs
          val corpus = rawData.select("features").rdd.zipWithIndex()
            .map { case (row: Row, index: Long) => (index, row.getAs[Vector](0)) }.cache()

          // Cluster the documents into n topics using LDA
          val ldaModel = new LDA().setK(ldaTopic)
            .setCheckpointInterval(30)
            .setOptimizer(ldaOptimizer)
            // 50/k +1
            .setDocConcentration(alpha)
            // 0.1 + 1 (em); 1.0 / k (online)
            .setTopicConcentration(beta)
            .run(corpus)
            .asInstanceOf[DistributedLDAModel]

          corpus.unpersist()
          // Output topics. Each is a distribution over words (matching word count vectors)
          resultsLog.info(s"LDA :: Learned $ldaTopic topics (as distributions over vocab of ${ldaModel.vocabSize} words)")

          val transformed = rawData.rdd.zipWithIndex().map(_.swap).join(ldaModel.topicDistributions).map {
            point =>
              val index = point._1
              val dataPair = point._2
              val row = dataPair._1
              val topics = dataPair._2
              val component_id = row.getAs[Int]("component_id")
              val product_id = row.getAs[Int]("product_id")
              val assignment_class = row.getAs[Double]("assignment_class")
              (index, datasetToLabeledPoint(FeatureContext(Map.empty[String, (Int, Int)]), component_id, product_id, topics.toSparse, assignment_class))
          }.sortByKey()

          val trainingData = transformed.filter(_._1 <= trainingDataCount).map(_._2)
          val testData = transformed.filter(_._1 > trainingDataCount).map(_._2)

          trainingData.saveAsObjectFile(s"$fsRoot/acf_training_data_LDA_$ldaTopic")
          testData.saveAsObjectFile(s"$fsRoot/acf_test_data_LDA_$ldaTopic")

          inputDataSVM.put(s"LDA_$ldaTopic", (trainingData, testData))

        }
      }

    } else {

      if (simple) {
        val trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_simple")
        val testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_simple")

        inputDataSVM.put("simple", (trainingData, testData))
      }

      if (pca) {
        val trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_PCA_100")
        val testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_PCA_100")

        inputDataSVM.put("PCA", (trainingData, testData))
      }

      if (chi2) {
        val trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_CHI2_$chi2Features")
        val testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_CHI2_$chi2Features")

        inputDataSVM.put("CHI2", (trainingData, testData))
      }

      if (lda) {
        ldaTopics map { ldaTopic =>

          val trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_LDA_$ldaTopic")
          val testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_LDA_$ldaTopic")

          inputDataSVM.put(s"LDA_$ldaTopic", (trainingData, testData))
        }
      }

    }

    // Run training algorithm to build the model
    val undersample = conf.getBoolean("preprocess.undersampling")

    // TRAIN and predict
    val SVMModels = inputDataSVM flatMap {
      elector =>
        // Elector ID (simple, PCA, CHI2, LDA)
        val key = elector._1

        // Train an SVM model for this elector

        val normalize = conf.getBoolean("postprocess.normalize")

        val (trainingData, testData) = if (normalize) {
          val _trainingData = elector._2._1
          val _testData = elector._2._2
          val scaler = new feature.StandardScaler().fit((_trainingData union _testData).map(x => x.features))
          (_trainingData.map(p => p.copy(features = scaler.transform(p.features))),
            _testData.map(p => p.copy(features = scaler.transform(p.features))))
          // Training and test data for this elector
        } else {
          // Training and test data for this elector
          (elector._2._1, elector._2._2)
        }

        val ldaModels = conf.getInt("preprocess.ldaModels")
        (1 to ldaModels) map {
          i =>
            val model = new SVMWithSGDMulticlass(undersample, i * 12345L).train(trainingData, 100, 1, 0.01, 1)

            // TestData :: (index,classLabel) -> Seq(prediction)
            testData.zipWithIndex().map(_.swap).map(data =>
              ((data._1, data._2.label), Seq(model.predict(data._2.features))))
        }
    }

    // Let's vote
    val predictionAndLabels = SVMModels.reduce((predictions1, predictions2) =>
      predictions1.join(predictions2)
        .map { joined =>
          // Combine predictions
          (joined._1, joined._2._1 ++ joined._2._2)
        }
    )

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels.map { pl =>
      val label = pl._1._2
      val prediction = pl._2.groupBy(identity).maxBy(_._2.size)._1
      // if (label != prediction) {
      //  println(s"$label, ($prediction)  ---  ${pl._2.mkString(",")}")
      // }
      (label, prediction)
    })

    val fMeasure = metrics.fMeasure
    val weightedPrecision = metrics.weightedPrecision
    val weightedRecall = metrics.weightedRecall
    val weightedFMeasure = metrics.weightedFMeasure

    resultsLog.info(s"withProduct: $includeProduct ; scalingFactor = $productScalingFactor, multiplier $productMultiplier")
    resultsLog.info(s"withCategory: $includeCategory ; categoryScalingFactor = $categoryScalingFactor, multiplier $categoryMultiplier")
    resultsLog.info("fMeasure = " + fMeasure)
    resultsLog.info("Weighted Precision = " + weightedPrecision)
    resultsLog.info("Weighted Recall = " + weightedRecall)
    resultsLog.info("Weighted fMeasure = " + weightedFMeasure)
    // println("Confusion Matrix = " + metrics.confusionMatrix.toString(500, 10000))

    // Step 3...Infinity - TDB

    val stopHere = true

    logger.debug(s"We're done in ${(System.currentTimeMillis() - startTime) / 1000} seconds!")
  }


  /**
    * If transform : TF/IDF, FREQUENCY-FILTERING, SAVE DATAFRAME else : LOAD DATAFRAME
    *
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
      val smoothTF = conf.getBoolean("transform.smoothTF")
      val timeDecay = conf.getBoolean("transform.timeDecay")

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
        .filter(pair => pair._2 > maxDocFreq || pair._2 < minDocFreq || pair._1.length < 2)
        .map(pair => pair._1).collect()

      val stopWordsRemover = new StopWordsRemover().setInputCol("words").setOutputCol("filteredwords")
        .setStopWords(stopWords)
      //.setStopWords(Array.empty[String])

      val cleanedData = stopWordsRemover.transform(wordsData)
      val cleanedVocabulary = cleanedData.map(r => r.getAs[mutable.WrappedArray[String]]("filteredwords"))
        .flatMap(words => words map (word => (word, 1)))
        .reduceByKey(_ + _)

      val vocabularySize = cleanedVocabulary.distinct().count()
      logger.debug(s"Vocabulary size $vocabularySize")

      val distinctWords = sc.broadcast(cleanedVocabulary.map(_._1).distinct().collect())

      val tfFunction: ((Iterable[_], Timestamp) => Vector) = (document: Iterable[_], timestamp: Timestamp) => {
        val distinctWordsValue = distinctWords.value
        val termFrequencies = mutable.HashMap.empty[Int, Double]

        // 1458950400000L is 26.03.2016 :)
        val scaleFactor = if (timeDecay) Math.log((1458950400000L - timestamp.getTime) / 3600000.0) / 10.0 else 1.0
        document.foreach { term =>
          val i = distinctWordsValue.indexOf(term)
          termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0 / scaleFactor)
        }
        if (smoothTF) {
          val max = if (termFrequencies.isEmpty) 1.0 else termFrequencies.maxBy(_._2)._2
          termFrequencies.map(freq => (freq._1, 0.5 + freq._2 * 0.5 / max))
          Vectors.sparse(distinctWordsValue.length, termFrequencies.map(freq => (freq._1, 0.5 + freq._2 * 0.5 / max)).toSeq)
        } else {
          Vectors.sparse(distinctWordsValue.length, termFrequencies.toSeq)
        }
      }
      val udf_tfFunction = functions.udf(tfFunction)
      val featurizedData = cleanedData.withColumn("rawFeatures", udf_tfFunction(cleanedData.col("filteredwords"), cleanedData.col("creation_ts"))).cache()

      val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
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

case class FeatureContext(features: Map[String, (Int, Int)])
