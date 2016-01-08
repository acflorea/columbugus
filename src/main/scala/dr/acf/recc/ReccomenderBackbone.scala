package dr.acf.recc

import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import dr.acf.connectors.MySQLConnector
import dr.acf.spark._
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature
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
object ReccomenderBackbone extends SparkOps with MySQLConnector {

  // cofig stuff
  val conf = ConfigFactory.load()

  val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]) {
    import sqlContext.implicits._

    logger.debug("Start!")
    val startTime = System.currentTimeMillis()

    // Execution parameters
    val cleansing = conf.getBoolean("phases.cleansing")
    val transform = conf.getBoolean("phases.transform")
    val training = conf.getBoolean("phases.training")
    val testing = conf.getBoolean("phases.testing")
    val pca = conf.getBoolean("phases.pca")

    // Charge configs
    val tokenizerType = conf.getInt("global.tokenizerType")
    val minWordSize = conf.getInt("global.minWordSize")

    // Step 1 - load data from DB

    val wordsData = if (cleansing) {
      logger.debug("CLEANSING :: Start!")
      val currentTime = System.currentTimeMillis()

      val bugInfoRDD: RDD[BugData] = buildBugsRDD

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
      _wordsData.write.mode("overwrite").parquet("acf_cleaned_data")
      logger.debug(s"CLEANSING :: " +
        s"Done in ${(System.currentTimeMillis() - currentTime) / 1000} seconds!")
      sqlContext.read.parquet("acf_cleaned_data")
    }
    else {
      logger.debug("CLEANSING :: Skip!")
      sqlContext.read.parquet("acf_cleaned_data")
    }

    val rescaledData = if (transform) {
      logger.debug("TRANSFORM :: Start!")

      val minDocFreq = conf.getInt("transform.minDocFreq")

      val currentTime = System.currentTimeMillis()

      val tst = wordsData.map(r => r.getAs[mutable.WrappedArray[String]]("words").toSet[String])
      val vocabularySize = tst.reduce((w1, w2) => w1.union(w2)).size
      logger.debug(s"Vocabulary size $vocabularySize")

      val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
        .setNumFeatures(vocabularySize)

      val featurizedData = hashingTF.transform(wordsData).cache()

      hashingTF.getNumFeatures

      val idf = new IDF().setMinDocFreq(minDocFreq).setInputCol("rawFeatures").setOutputCol("features")
      val idfModel = idf.fit(featurizedData)

      val _rescaledData = idfModel.transform(featurizedData)

      _rescaledData.write.mode("overwrite").parquet("acf_numerical_data")
      logger.debug(s"TRANSFORM :: " +
        s"Done in ${(System.currentTimeMillis() - currentTime) / 1000} seconds!")
      sqlContext.read.parquet("acf_numerical_data")

    } else {
      logger.debug("CLEANSING :: Skip!")
      sqlContext.read.parquet("acf_numerical_data")
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

        val allData = rescaledData.select("index", "component_id", "features", "assignment_class")
          .map(rowToLabeledPoint)
          //.filter(p => p.label < 40.0)
          .zipWithIndex()

        // Split data into training (90%) and test (10%).
        val trainingCount = allData.count() / 10 * 9 toInt

        val trainingData = allData.filter(_._2 <= trainingCount).map(_._1).cache()
        // keep most recent 10% of data for testing
        val testData = allData.filter(_._2 > trainingCount).map(_._1)

        logger.debug(s"Training data size ${trainingData.count()}")
        logger.debug(s"Test data size ${testData.count()}")

        val (trainingProjected, testProjected) = if (pca) {
          // Compute the top 10 principal components.
          val PCAModel = new feature.PCA(100).fit(trainingData.map(_.features))
          // Project vectors to the linear space spanned by the top 10 principal components, keeping the label
          (trainingData.map(p => p.copy(features = PCAModel.transform(p.features))),
            testData.map(p => p.copy(features = PCAModel.transform(p.features))))
        }
        else {
          (trainingData, testData)
        }

        // Run training algorithm to build the model
        val model = new SVMWithSGDMulticlass().train(trainingProjected, 100, 1, 0.01, 1)

        // Compute raw scores on the test set.
        val predictionAndLabels = testProjected.map { case LabeledPoint(label, features) =>
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

  /**
    * Returns an RDD containing (bug_id, bug_details) type of data
    * The details are in the form t<timestamp>:: info
    * (the description and comments are included)
    * @return
    */
  def buildBugsRDD: RDD[BugData] = {

    val use_assigned_to = false

    // Charge configs
    val testMode = conf.getBoolean("global.testMode")
    val includeComments = conf.getBoolean("global.includeComments")
    val issuesThreshold = conf.getInt("global.issuesThreshold")
    val timeThreshold = conf.getInt("global.timeThreshold")
    val timeIntervals = conf.getInt("global.timeIntervals")
    val timeThresholdForTraining = conf.getInt("global.timeThresholdForTraining")

    val testFilter =
      (column: String) => if (testMode) s"$column > 300000 " else "1 = 1 "

    val resolutionFilter =
      (prefix: String) => s"${prefix}resolution = 'FIXED'"

    val minIssuesFilter =
      (prefix: String) => s"${prefix}bugs_assigned > '$issuesThreshold'"

    val mostRecentDatesWithDelta = 0 to timeIntervals map { i =>
      mySQLDF(
        s"(select DATE_SUB(max(delta_ts), INTERVAL ${timeThreshold * (timeIntervals - i)} DAY) " +
          s"from bugs) as maxDate").
        collect().head.getTimestamp(0)
    }

    val oldestValidDate =
      mySQLDF(
        s"(select DATE_SUB(max(delta_ts), INTERVAL $timeThresholdForTraining DAY) " +
          s"from bugs) as maxDate").
        collect().head.getTimestamp(0)

    val dateFilter =
      (interval: Int, prefix: String) =>
        s"${prefix}delta_ts <= '${mostRecentDatesWithDelta(interval + 1)}' " +
          s"and ${prefix}delta_ts > '${mostRecentDatesWithDelta(interval)}'"

    val dateFilterAssign =
      (interval: Int, prefix: String) =>
        s"${prefix}bug_when <= '${mostRecentDatesWithDelta(interval + 1)}' " +
          s"and ${prefix}bug_when > '${mostRecentDatesWithDelta(interval)}'"

    // Assignment data - all users with more than "issuesThreshold" items fixed
    // after "mostRecentDateWithDelta"
    val bugAssignmentDataFrame = (0 until timeIntervals map { i =>
      if (use_assigned_to)
        mySQLDF(
          "(" +
            "select b.assigned_to, count(*) as bugs_assigned " +
            "from bugs b " +
            "where " + testFilter("b.bug_id") +
            " AND " + resolutionFilter("b.") + " " +
            " AND " + dateFilter(i, "b.") + " " +
            " AND b.bug_id not in (select d.dupe from duplicates d) " +
            "group by b.assigned_to " +
            "having " + minIssuesFilter("") + " " +
            "order by bugs_assigned desc" +
            ") as bugslice"
        )
      else
        mySQLDF(
          "(" +
            "select ba.who as assigned_to, count(*) as bugs_assigned " +
            "from bugs_activity ba " +
            "where " + testFilter("ba.bug_id") +
            " AND ba.fieldid = '11' and ba.added = 'FIXED' " +
            " AND " + dateFilterAssign(i, "ba.") + " " +
            " AND ba.bug_id not in (select d.dupe from duplicates d) " +
            "group by ba.who " +
            "having " + minIssuesFilter("") + " " +
            "order by bugs_assigned desc" +
            ") as bugslice"
        )
    }).reduce {
      (df1, df2) => df1.join(df2, "assigned_to")
    }

    val bugAssignmentData = bugAssignmentDataFrame.map(row =>
      BugAssignmentData(row.getInt(0), (1 to timeIntervals map (i => row.getLong(i))).sum))

    val assignments = bugAssignmentData.collect.
      zipWithIndex.map(elem => elem._1.assigned_to -> elem._2).toMap

    // Duplicates -- Which bugs are duplicates of which other bugs.
    val bugsDuplicatesDataFrame = mySQLDF(
      "(" +
        "select d.dupe_of, d.dupe " +
        "from duplicates d " +
        "where " + testFilter("d.dupe_of") +
        ") as bugduplicates"
    )

    // Main bug data
    val bugsDataFrame = if (use_assigned_to) mySQLDF(
      "(" +
        "select b.bug_id, b.creation_ts, b.short_desc," +
        "b.bug_status, b.assigned_to, b.component_id, b.bug_severity, " +
        "b.resolution, b.delta_ts, " +
        "c.name as component_name, c.product_id, " +
        "p.name as product_name, p.classification_id, " +
        "cl.name as classification_name " +
        "from bugs b " +
        "join components c on b.component_id = c.id " +
        "join products p on c.product_id = p.id " +
        "join classifications cl on p.classification_id = cl.id " +
        "where " + testFilter("b.bug_id") +
        " AND " + resolutionFilter("b.") + " " +
        " AND b.delta_ts > '" + oldestValidDate + "'" +
        " AND b.bug_id not in (select d.dupe from duplicates d) " +
        ") as bugslice"
    )
    else
      mySQLDF(
        "(" +
          "select b.bug_id, b.creation_ts, b.short_desc," +
          "b.bug_status, ba.who as assigned_to, b.component_id, b.bug_severity, " +
          "b.resolution, b.delta_ts, " +
          "c.name as component_name, c.product_id, " +
          "p.name as product_name, p.classification_id, " +
          "cl.name as classification_name " +
          "from bugs b " +
          "join bugs_activity ba on b.bug_id = ba.bug_id and ba.fieldid = '11' and ba.added='FIXED' " +
          "join components c on b.component_id = c.id " +
          "join products p on c.product_id = p.id " +
          "join classifications cl on p.classification_id = cl.id " +
          "where " + testFilter("b.bug_id") +
          " AND " + resolutionFilter("b.") + " " +
          " AND b.delta_ts > '" + oldestValidDate + "'" +
          " AND b.bug_id not in (select d.dupe from duplicates d) " +
          ") as bugslice"
      )

    // The meat of bugzilla -- here is where all user comments are stored!
    val bugsLongdescsDataFrame = mySQLDF(
      "(" +
        "select l.bug_id, l.bug_when, l.thetext " +
        "from longdescs l " +
        "where " + testFilter("l.bug_id") +
        " AND l.bug_id not in (select d.dupe from duplicates d) " +
        ") as buglongdescsslice"
    )

    // Bugs fulltext
    val bugsFulltextDataFrame = mySQLDF(
      "(" +
        "select l.bug_id, l.comments " +
        "from bugs_fulltext l " +
        "where " + testFilter("l.bug_id") +
        " AND l.bug_id not in (select d.dupe from duplicates d) " +
        ") as bugfulltextslice"
    )

    // ($bug_id,($comment)...)
    val bugsLongdescsRDD =
      if (includeComments) {
        bugsLongdescsDataFrame.
          map(row => (row.getInt(0), row.getString(2))).
          reduceByKey((c1, c2) => s"$c1\n$c2").
          map(row => (row._1, Row(row._2)))
      }
      else {
        bugsFulltextDataFrame.
          map(row => (row.getInt(0), row.getString(1))).
          groupByKey().
          map(row => (row._1, Row(row._2.head)))
      }

    // ($bug_id,t$timestamp:: $short_desc)
    val idAndDescRDD = bugsDataFrame.select("bug_id", "short_desc",
      "bug_status", "assigned_to", "component_id", "bug_severity", "creation_ts").
      map(row => (row.getInt(0), Row(row.getString(1),
        row.getString(2), row.getInt(3), row.getInt(4), row.getString(5), row.getTimestamp(6))))

    // join dataframes
    val bugsRDD = idAndDescRDD.
      leftOuterJoin[Row](bugsLongdescsRDD).
      map { row =>
        val key = row._1
        val r1 = row._2._1
        val r2Opt = row._2._2
        r2Opt match {
          case Some(r2) =>
            BugData(
              -1,
              key,
              s"${r1.getString(0)}\n${r2.getString(0)}",
              r1.getString(1), // "bug_status"
              r1.getInt(2), // "assigned_to"
              assignments.getOrElse(r1.getInt(2), -1).toDouble,
              r1.getInt(3), // "component_id"
              r1.getString(4), // "bug_severity"
              r1.getTimestamp(5)
            )
          case None =>
            BugData(
              -1,
              key,
              r1.getString(0),
              r1.getString(1),
              r1.getInt(2),
              assignments.getOrElse(r1.getInt(2), -1).toDouble,
              r1.getInt(3),
              r1.getString(4),
              r1.getTimestamp(5)
            )
        }
      }
      // Sort by creation time
      .sortBy(bugData => bugData.creation_ts.getTime)
      // Add index
      .zipWithIndex()
      // Stick the index into BugData
      .map {
      rowWithIndex => rowWithIndex._1.copy(index = rowWithIndex._2)
    }

    bugsRDD

  }
}

case class BugData(index: Long, bug_id: Integer, bug_data: String,
                   bug_status: String, assigned_to: Integer,
                   assignment_class: Double,
                   component_id: Integer, bug_severity: String,
                   creation_ts: Timestamp)

case class BugAssignmentData(assigned_to: Integer, no: Long)

