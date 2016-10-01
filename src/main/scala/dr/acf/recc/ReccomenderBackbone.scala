package dr.acf.recc

import java.sql.Timestamp

import com.typesafe.config.ConfigRenderOptions
import dr.acf.extractors.DBExtractor
import dr.acf.spark.SparkOps._
import dr.acf.spark._
import dr.acf.spark.evaluation.MulticlassMultilabelMetrics
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.ml.clustering
import org.apache.spark.mllib.tree.RandomForest

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

    val chi2Features = conf.getInt("preprocess.chi2Features")
    val ldaTopics = conf.getString("preprocess.ldaTopics").split(",").map(_.trim.toInt)
    val ldaOptimizer = conf.getString("preprocess.ldaOptimizer")

    val inputDataSVM = mutable.Map.empty[String, (RDD[LabeledPoint], RDD[LabeledPoint])]

    val categorySFSize = if (includeCategory) conf.getString("preprocess.categoryScalingFactor").split(",").length - 1 else 0
    val categoryMSize = if (includeCategory) conf.getString("preprocess.categoryMultiplier").split(",").length - 1 else 0
    val productSFSize = if (includeProduct) conf.getString("preprocess.productScalingFactor").split(",").length - 1 else 0
    val productMSize = if (includeProduct) conf.getString("preprocess.productMultiplier").split(",").length - 1 else 0

    for {categorySFIndex <- 0 to categorySFSize
         categoryMIndex <- 0 to categoryMSize
         productSFIndex <- 0 to productSFSize
         productMIndex <- 0 to productMSize
    } {

      if (preprocess) {

        val normalize = conf.getBoolean("preprocess.normalize")
        val useCategorical = conf.getBoolean("preprocess.useCategorical")

        // Integrate more features
        val compIds = if (includeCategory) scaledData.select("component_id").map(_.getAs[Int](0)).distinct().collect() else Array.empty[Int]
        val prodIds = if (includeProduct) scaledData.select("product_id").map(_.getAs[Int](0)).distinct().collect() else Array.empty[Int]

        def datasetToLabeledPoint = (featureContext: FeatureContext, component_id: Int, product_id: Int, features: Vector, assignment_class: Double) => {

          val _includeCategory = featureContext.features.get("category").isDefined
          val _includeProduct = featureContext.features.get("product").isDefined

          val labeledPoint = if (_includeCategory || _includeProduct) {

            val (_productScalingFactor, _productMultiplier) =
              if (_includeProduct) featureContext.features.get("product").get else (0, 0)
            val (_categoryScalingFactor, _categoryMultiplier) =
              if (_includeCategory) featureContext.features.get("category").get else (0, 0)

            if (useCategorical) {
              features match {
                case sparse: SparseVector =>

                  val productIndex = if (_includeProduct) Array(prodIds.indexOf(product_id).toDouble) else Array.emptyDoubleArray
                  val productSize = productIndex.length
                  val productIndices = productIndex.map(_ => 0)

                  val categoryIndex = if (_includeCategory) Array(compIds.indexOf(component_id).toDouble) else Array.emptyDoubleArray
                  val categorySize = categoryIndex.length
                  val categoryIndices = categoryIndex.map(_ => productIndices.length)

                  LabeledPoint(
                    assignment_class,
                    Vectors.sparse(
                      productSize + categorySize + features.size,
                      Array.concat(
                        productIndices,
                        categoryIndices,
                        sparse.indices map (i => i + categorySize + productSize)
                      ),
                      Array.concat(productIndex, categoryIndex, sparse.values)
                    )
                  )

                case dense: DenseVector =>

                  val categoryIndex = if (_includeCategory) Array(compIds.indexOf(component_id).toDouble) else Array.emptyDoubleArray
                  val productIndex = if (_includeProduct) Array(prodIds.indexOf(product_id).toDouble) else Array.emptyDoubleArray

                  LabeledPoint(
                    assignment_class,
                    Vectors.dense(Array.concat(categoryIndex, productIndex, dense.values))
                  )

              }

            } else {

              features match {
                case sparse: SparseVector =>

                  val productSize = prodIds.length * _productMultiplier
                  val productIndices = if (_includeProduct)
                    1 to _productMultiplier map (i => prodIds.length * (i - 1) + prodIds.indexOf(product_id))
                  else
                    Vector.empty
                  val productScalingArray = if (_includeProduct)
                    1 to _productMultiplier map (i => _productScalingFactor.toDouble)
                  else
                    Vector.empty

                  val categorySize = compIds.length * _categoryMultiplier
                  val categoryIndices = 1 to _categoryMultiplier map (i => productSize + compIds.length * (i - 1) + compIds.indexOf(component_id))
                  val categoryScalingArray = 1 to _categoryMultiplier map (i => _categoryScalingFactor.toDouble)

                  LabeledPoint(
                    assignment_class,
                    Vectors.sparse(
                      productSize + categorySize + features.size,
                      Array.concat(
                        productIndices.toArray,
                        categoryIndices.toArray,
                        sparse.indices map (i => i + prodIds.length + compIds.length)
                      ),
                      Array.concat(productScalingArray.toArray, categoryScalingArray.toArray, sparse.values)
                    )
                  )

                case dense: DenseVector =>

                  val oneCategorySeries = compIds.map(i => if (i == component_id) _categoryScalingFactor.toDouble else 0.0)
                  val oneProductSeries = prodIds.map(i => if (i == product_id) _productScalingFactor.toDouble else 0.0)

                  val categorySeries = Seq.fill(_categoryMultiplier)(oneCategorySeries).flatten.toArray
                  val productSeries = Seq.fill(_productMultiplier)(oneProductSeries).flatten.toArray

                  LabeledPoint(
                    assignment_class,
                    Vectors.dense(Array.concat(categorySeries, productSeries, dense.values))
                  )

              }
            }

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
          val features = row.getAs[Vector]("features")
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

        resultsLog.info(s"Training data size $trainingDataCount")
        resultsLog.info(s"Test data size ${allDataCount - trainingDataCount}")

        val rawTestData = rescaledData.filter(s"index > $trainingDataCount").cache()
        val rawTrainingData = rescaledData.filter(s"index <= $trainingDataCount").cache()

        // Simple model
        if (simple) {
          logger.debug("Training simple model")

          val featureContext: FeatureContext = getFeatureContext(categorySFIndex, categoryMIndex, productSFIndex, productMIndex)

          val trainingData = rawTrainingData.map(rowToLabeledPoint(featureContext, _))
          val testData = rawTestData.map(rowToLabeledPoint(featureContext, _))

          trainingData.saveAsObjectFile(s"$fsRoot/acf_training_data_simple_${FileFriendly(featureContext.features.toString)}")
          testData.saveAsObjectFile(s"$fsRoot/acf_test_data_simple_${FileFriendly(featureContext.features.toString)}")

          inputDataSVM.put(s"simple ${featureContext.features.toString}", (trainingData, testData))
        }

        // PCA
        if (pca) {

          // @deprecated

          logger.debug("Training PCA model")

          /** PCA */
          // Compute the top 100 principal components.
          val pca = new PCA()
            .setInputCol("features")
            .setOutputCol("pcaFeatures")
            .setK(100)
            .fit(rawTrainingData)

          val featureContext: FeatureContext = getFeatureContext(categorySFIndex, categoryMIndex, productSFIndex, productMIndex)

          // Project vectors to the linear space spanned by the top 10 principal components, keeping the label
          val trainingData = pca.transform(rawTrainingData).drop("features")
            .withColumnRenamed("pcaFeatures", "features").map(rowToLabeledPoint(featureContext, _))
          val testData = pca.transform(rawTestData).drop("features")
            .withColumnRenamed("pcaFeatures", "features").map(rowToLabeledPoint(featureContext, _))

          trainingData.saveAsObjectFile(s"$fsRoot/acf_training_data_PCA_100_${FileFriendly(featureContext.features.toString)}")
          testData.saveAsObjectFile(s"$fsRoot/acf_test_data_PCA_100_${FileFriendly(featureContext.features.toString)}")

          inputDataSVM.put(s"PCA ${featureContext.features.toString}", (trainingData, testData))
        }

        // CHI2
        if (chi2) {
          logger.debug("Training CHI2 model")

          // Create ChiSqSelector that will select top chi2Features features
          val selector = new ChiSqSelector(chi2Features)
          // Create ChiSqSelector model (selecting features)
          val cached = rawTrainingData.select("assignment_class", "features")
            .map { case Row(label: Double, v: Vector) => LabeledPoint(label, v) }.cache()
          val model = selector.fit(cached)

          val transformer = sc.broadcast(model)
          cached.unpersist()

          val tfFunction: ((Vector) => Vector) = (document: Vector) => {
            val chi2transformer = transformer.value
            chi2transformer.transform(document)
          }
          val udf_tfFunction = functions.udf(tfFunction)

          val featureContext: FeatureContext = getFeatureContext(categorySFIndex, categoryMIndex, productSFIndex, productMIndex)

          // Filter the top features from each feature vector
          val testData = rawTestData.withColumn("CHIFeatures", udf_tfFunction(rawTestData.col("features")))
            .drop("features").withColumnRenamed("CHIFeatures", "features").map(rowToLabeledPoint(featureContext, _))

          val trainingData = rawTrainingData.withColumn("CHIFeatures", udf_tfFunction(rawTrainingData.col("features")))
            .drop("features").withColumnRenamed("CHIFeatures", "features").map(rowToLabeledPoint(featureContext, _))


          trainingData.saveAsObjectFile(s"$fsRoot/acf_training_data_CHI2_${chi2Features}_${FileFriendly(featureContext.features.toString)}")
          testData.saveAsObjectFile(s"$fsRoot/acf_test_data_CHI2_${chi2Features}_${FileFriendly(featureContext.features.toString)}")

          inputDataSVM.put(s"CHI2 ${featureContext.features.toString}", (trainingData, testData))
        }

        // LDA
        if (lda) {

          ldaTopics map { ldaTopic =>

            logger.debug("Training LDA model")

            val alpha = conf.getDouble("preprocess.ldaAlpha")
            val beta = conf.getDouble("preprocess.ldaBeta")

            val indexedData = rawData.rdd.zipWithIndex().map(_.swap).sortByKey()

            val featureContext: FeatureContext = getFeatureContext(categorySFIndex, categoryMIndex, productSFIndex, productMIndex)

            // check if a model already exists for this combination of features
            val existingModel = try {
              Some(clustering.DistributedLDAModel.load(s"$fsRoot/acf_LDAMODEL_$ldaTopic"))
            } catch {
              case ex: InvalidInputException => None
            }

            // Cluster the documents into n topics using LDA
            val ldaModel = if (existingModel.isDefined) existingModel.get
            else {

              val lda = new clustering.LDA()
                .setOptimizer(ldaOptimizer)
                .setFeaturesCol("features")
                .setK(ldaTopic)
                .setCheckpointInterval(10)
                .setTopicDistributionCol("LDAfeatures")
              val _ldaModel = lda.fit(rawTrainingData)

              // Output topics. Each is a distribution over words (matching word count vectors)
              resultsLog.info(s"LDA :: Learned $ldaTopic topics (as distributions over vocab of ${_ldaModel.vocabSize} words)")

              _ldaModel.save(s"$fsRoot/acf_LDAMODEL_$ldaTopic")
              _ldaModel
            }

            val trainingData = ldaModel.transform(rawTrainingData)
              .rdd.map {
              row =>
                val topics = row.getAs[Vector]("LDAfeatures")
                val component_id = row.getAs[Int]("component_id")
                val product_id = row.getAs[Int]("product_id")
                val assignment_class = row.getAs[Double]("assignment_class")
                datasetToLabeledPoint(featureContext, component_id, product_id, topics, assignment_class)
            }.cache()

            val testData = ldaModel.transform(rawTestData)
              .rdd.map {
              row =>
                val topics = row.getAs[Vector]("LDAfeatures")
                val component_id = row.getAs[Int]("component_id")
                val product_id = row.getAs[Int]("product_id")
                val assignment_class = row.getAs[Double]("assignment_class")
                datasetToLabeledPoint(featureContext, component_id, product_id, topics, assignment_class)
            }.cache()

            trainingData.saveAsObjectFile(s"$fsRoot/acf_training_data_LDA_${ldaTopic}_${FileFriendly(featureContext.features.toString)}")
            testData.saveAsObjectFile(s"$fsRoot/acf_test_data_LDA_${ldaTopic}_${FileFriendly(featureContext.features.toString)}")

            inputDataSVM.put(s"LDA_$ldaTopic ${featureContext.features.toString}", (trainingData, testData))

          }
        }

      } else {

        if (simple) {

          val featureContext: FeatureContext = getFeatureContext(categorySFIndex, categoryMIndex, productSFIndex, productMIndex)

          val trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_simple_${FileFriendly(featureContext.features.toString)}")
          val testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_simple_${FileFriendly(featureContext.features.toString)}")

          inputDataSVM.put(s"simple ${featureContext.features.toString}", (trainingData, testData))
        }

        if (pca) {

          val featureContext: FeatureContext = getFeatureContext(categorySFIndex, categoryMIndex, productSFIndex, productMIndex)

          val trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_PCA_100_${FileFriendly(featureContext.features.toString)}")
          val testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_PCA_100_${FileFriendly(featureContext.features.toString)}")

          inputDataSVM.put(s"PCA ${featureContext.features.toString}", (trainingData, testData))
        }

        if (chi2) {

          val featureContext: FeatureContext = getFeatureContext(categorySFIndex, categoryMIndex, productSFIndex, productMIndex)

          val trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_CHI2_${chi2Features}_${FileFriendly(featureContext.features.toString)}")
          val testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_CHI2_${chi2Features}_${FileFriendly(featureContext.features.toString)}")

          inputDataSVM.put(s"CHI2 ${featureContext.features.toString}", (trainingData, testData))
        }

        if (lda) {
          ldaTopics map { ldaTopic =>

            val featureContext: FeatureContext = getFeatureContext(categorySFIndex, categoryMIndex, productSFIndex, productMIndex)

            val trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_LDA_${ldaTopic}_${FileFriendly(featureContext.features.toString)}")
            val testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_LDA_${ldaTopic}_${FileFriendly(featureContext.features.toString)}")

            inputDataSVM.put(s"LDA_$ldaTopic ${featureContext.features.toString}", (trainingData, testData))
          }
        }

      }
    }

    val trainingStarts = System.currentTimeMillis

    // Run training algorithm to build the model
    val undersample = conf.getBoolean("preprocess.undersampling")

    // TRAIN and predict
    val SVMModels = inputDataSVM flatMap {
      elector =>
        // Elector ID (simple, PCA, CHI2, LDA)
        val key = elector._1

        // Train an SVM model for this elector

        val normalize = conf.getBoolean("postprocess.normalize")
        val removeOutliers = conf.getBoolean("postprocess.removeOutliers")

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

        // Filter everything above mean plus 2 * std
        val dataPerclass = trainingData.map(_.label).countByValue
        val classesRDD = sc.parallelize(dataPerclass.values.toList)
        val (filteredTrainingData, filteredTestData, classes) = if (removeOutliers) {

          val threshold = classesRDD.stdev() * 2 + classesRDD.mean()

          resultsLog.info(s"Compute class threshold std:${classesRDD.stdev()} + avg:${classesRDD.mean()}")

          val filteredClasses = dataPerclass.filter(p => p._2 < threshold)

          resultsLog.info(s"Keeping ${filteredClasses.size} out of ${dataPerclass.size} classes")

          (trainingData.filter(point => filteredClasses.contains(point.label)).cache()
            , testData.filter(point => filteredClasses.contains(point.label)).cache(),
            filteredClasses.size)
        } else {

          (trainingData.cache(), testData.cache(), dataPerclass.size)
        }

        // How many categorical features we have
        val categoricalFeaturesNo = Seq(includeCategory, includeProduct).collect { case true => 1 }.length
        // Build categorical featueres map
        val categoricalFeaturesInfo = (0 until categoricalFeaturesNo).foldLeft(Map.empty[Int, Int])((features: Map[Int, Int], index) =>
          features ++ Map(index -> filteredTrainingData.union(filteredTestData).map { point =>
            point.features match {
              case dense: DenseVector => dense.values(index)
              case sparse: SparseVector => sparse.values(index)
            }
          }.distinct().count().toInt)
        )

        val modelsNo = conf.getInt("preprocess.modelsNo")
        (1 to modelsNo) map {
          i =>

            // MLUtils.saveAsLibSVMFile(filteredTrainingData.repartition(1), s"$fsRoot/svmInputTrain")
            // MLUtils.saveAsLibSVMFile(filteredTestData.repartition(1), s"$fsRoot/svmInputTest" )

            val model = new SVMWithSGDMulticlass(undersample, i * 12345L).train(filteredTrainingData, 1000, 1, 0.01, 1)

            // Train a RandomForest model.
            // Empty categoricalFeaturesInfo indicates all features are continuous.
            val numClasses = classes
            val numTrees = 2
            val featureSubsetStrategy = "auto"
            val impurity = "gini"
            val maxDepth = 35
            val maxBins = 250

            //            val model = RandomForest.trainClassifier(filteredTrainingData, numClasses, categoricalFeaturesInfo,
            //              numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

            // Evaluate model on test instances and compute test error
            val labelAndPreds = testData.map { point =>
              val prediction = model.predict(point.features)
              (point.label, prediction)
            }

            // TestData :: (index,classLabel) -> Seq(prediction)
            (key, filteredTestData.zipWithIndex().map(_.swap).map(data =>
              ((data._1, data._2.label), Seq(model.predict(data._2.features)))))
        }
    }

    logger.debug(s"Training done in ${(System.currentTimeMillis() - trainingStarts) / 1000} seconds!")

    SVMModels foreach { SVMModel =>
      // Get evaluation metrics.
      val _metrics = new MulticlassMultilabelMetrics(SVMModel._2.map { pl =>
        val label = pl._1._2
        val prediction = pl._2.groupBy(identity).maxBy(_._2.size)._1
        (Seq(prediction), label)
      })

      logQualityMeasurements(SVMModel._1, _metrics)
    }

    // Let's vote
    val predictionAndLabels = SVMModels.values.reduce((predictions1, predictions2) =>
      predictions1.join(predictions2)
        .map { joined =>
          // Combine predictions
          (joined._1, joined._2._1 ++ joined._2._2)
        }
    )

    // Get evaluation metrics.
    val metrics = new MulticlassMultilabelMetrics(predictionAndLabels.map { pl =>
      val label = pl._1._2
      val prediction = pl._2.groupBy(identity).maxBy(_._2.size)._1
      (Seq(prediction), label)
    })

    logQualityMeasurements("AVERAGED", metrics)

    // Step 3...Infinity - TDB

    val stopHere = true

    logger.debug(s"We're done in ${(System.currentTimeMillis() - startTime) / 1000} seconds!")
  }

  /**
    * Utility method for logging quality metrics
    *
    * @param modelName
    * @param _metrics
    */
  private def logQualityMeasurements(modelName: String, _metrics: MulticlassMultilabelMetrics): Unit = {
    val _fMeasure = _metrics.fMeasure
    val _weightedPrecision = _metrics.weightedPrecision
    val _weightedRecall = _metrics.weightedRecall
    val _weightedFMeasure = _metrics.weightedFMeasure
    val _averagedPrecision = _metrics.averagedPrecision
    val _averagedRecall = _metrics.averagedRecall
    val _averagedFMeasure = _metrics.averagedFMeasure
    val _averagedAccuracy = _metrics.averagedAccuracy

    resultsLog.info(s"MODEL -> $modelName")
    // resultsLog.info("fMeasure = " + _fMeasure)
    resultsLog.info("Weighted Precision = " + _weightedPrecision)
    resultsLog.info("Weighted Recall = " + _weightedRecall)
    resultsLog.info("Weighted fMeasure = " + _weightedFMeasure)
    resultsLog.info("Averaged Precision = " + _averagedPrecision)
    resultsLog.info("Averaged Recall = " + _averagedRecall)
    resultsLog.info("Averaged fMeasure = " + _averagedFMeasure)
    // resultsLog.info("Averaged Accuracy = " + _averagedAccuracy)
  }

  /**
    * Hdfs files merger
    *
    * @param srcPath
    * @param dstPath
    */
  def merge(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  /**
    * A more reasonable file name candidate
    *
    * @param input - input name it it's raw form
    * @return - a more reasonable file name candidate
    */
  def FileFriendly(input: String): String = {
    input.replace(".", "-").replace(",", "-")
  }

  /**
    * Retrieves current features context (values for category, product...)
    *
    * @return
    */
  def getFeatureContext(categorySFIndex: Int = 0, categoryMIndex: Int = 0, productSFIndex: Int = 0, productMIndex: Int = 0): FeatureContext = {

    val includeCategory = conf.getBoolean("preprocess.includeCategory")
    val includeProduct = conf.getBoolean("preprocess.includeProduct")

    val categoryScalingFactor = conf.getString("preprocess.categoryScalingFactor").split(",").map(_.trim.toInt)
    val categoryMultiplier = conf.getString("preprocess.categoryMultiplier").split(",").map(_.trim.toInt)
    val productScalingFactor = conf.getString("preprocess.productScalingFactor").split(",").map(_.trim.toInt)
    val productMultiplier = conf.getString("preprocess.productMultiplier").split(",").map(_.trim.toInt)

    val features = Seq(includeCategory -> ("category", categoryScalingFactor(categorySFIndex), categoryMultiplier(categoryMIndex)),
      includeProduct -> ("product", productScalingFactor(productSFIndex), productMultiplier(productMIndex))).collect {
      case pair if pair._1 => pair._2._1 -> (pair._2._2, pair._2._3)
    }.toMap

    val featureContext = FeatureContext(features)
    featureContext
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

      val bugInfoDF = DBExtractor.buildBugsRDD.toDF()

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

}

case class FeatureContext(features: Map[String, (Int, Int)])
