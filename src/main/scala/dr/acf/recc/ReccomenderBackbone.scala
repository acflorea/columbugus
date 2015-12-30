package dr.acf.recc

import com.typesafe.config.ConfigFactory
import dr.acf.common.CategoricalVariable
import dr.acf.connectors.MySQLConnector
import dr.acf.spark._
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{SparseVector, Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

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
      val currentTime = System.currentTimeMillis()

      val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
      //.setNumFeatures(20)

      val featurizedData = hashingTF.transform(wordsData)
      val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
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
    val bugSeverity = rescaledData.select("bug_severity").map(_.getAs[String](0)).distinct().collect()

    // Step 2.a one vs all SVM
    val labeledPoints = rescaledData.select("component_id", "bug_severity", "features", "assignment_class").
      map { point =>
        val component_id = point.getAs[Int]("component_id")
        val bug_severity = point.getAs[String]("bug_severity")
        val features = point.getAs[SparseVector]("features")
        val assignment_class = point.getAs[Double]("assignment_class")
        val labeledPoint = LabeledPoint(
          assignment_class,
          Vectors.sparse(
            compIds.length + bugSeverity.length + features.size,
            Array(compIds.indexOf(component_id), bugSeverity.indexOf(bug_severity)) ++
              features.indices map (i => i + compIds.length + bugSeverity.length),
            Array(1.0, 1.0) ++ features.values
          )
        )
        labeledPoint
      }


    // Split data into training (90%) and test (10%).
    val splits = labeledPoints.randomSplit(Array(0.9, 0.1), seed = 123456789L)
    val trainingData = splits(0).cache()
    val testData = splits(1)

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


    println("fMeasure = " + fMeasure)
    println("Weighted Precision = " + weightedPrecision)
    println("Weighted Recall = " + weightedRecall)
    println("Weighted fMeasure = " + weightedFMeasure)


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

    // Charge configs
    val testMode = conf.getBoolean("global.testMode")
    val includeComments = conf.getBoolean("global.includeComments")
    val issuesThreshold = conf.getInt("global.issuesThreshold")
    val timeThreshold = conf.getInt("global.timeThreshold")

    val testFilter =
      (column: String) => if (testMode) s"$column > 300000 " else "1 = 1 "

    val resolutionFilter =
      (prefix: String) => s"${prefix}resolution = 'FIXED'"

    val minIssuesFilter =
      (prefix: String) => s"${prefix}bugs_assigned > '$issuesThreshold'"

    val mostRecentDateWithDelta = mySQLDF(
      s"(select DATE_SUB(max(delta_ts), INTERVAL $timeThreshold DAY) from bugs) as maxDate").
      collect().head.getTimestamp(0)

    val dateFilter =
      (prefix: String) => s"${prefix}delta_ts > '$mostRecentDateWithDelta'"

    // Assignment data - all users with more than "issuesThreshold" items fixed
    // after "mostRecentDateWithDelta"
    val bugAssignmentDataFrame = mySQLDF(
      "(" +
        "select b.assigned_to, count(*) as bugs_assigned " +
        "from bugs b " +
        "where " + testFilter("b.bug_id") +
        " AND " + resolutionFilter("b.") + " " +
        " AND " + dateFilter("b.") + " " +
        "group by b.assigned_to " +
        "having " + minIssuesFilter("") + " " +
        "order by bugs_assigned desc" +
        ") as bugslice"
    )

    // Main bug data
    val bugsDataFrame = mySQLDF(
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
        ") as bugslice"
    )

    // Duplicates -- Which bugs are duplicates of which other bugs.
    val bugsDuplicatesDataFrame = mySQLDF(
      "(" +
        "select d.dupe_of, d.dupe " +
        "from duplicates d " +
        "where " + testFilter("d.dupe_of") +
        ") as bugduplicates"
    )

    // The meat of bugzilla -- here is where all user comments are stored!
    val bugsLongdescsDataFrame = mySQLDF(
      "(" +
        "select l.bug_id, l.bug_when, l.thetext " +
        "from longdescs l " +
        "where " + testFilter("l.bug_id") +
        ") as buglongdescsslice"
    )

    // ($bug_id,t$timestamp:: $short_desc)
    val idAndDescRDD = bugsDataFrame.select("bug_id", "creation_ts", "short_desc",
      "bug_status", "assigned_to", "component_id", "bug_severity").
      map(row => (row.getInt(0), Row(s"t${row.getTimestamp(1).getTime}:: ${row.getString(2)}",
        row.getString(3), row.getInt(4), row.getInt(5), row.getString(6))))

    // ($bug_id,(t$timestamp:: $comment)...)
    val bugsLongdescsRDD =
      if (includeComments) {
        bugsLongdescsDataFrame.
          map(row => (row.getInt(0), s"t${row.getTimestamp(1).getTime}:: ${row.getString(2)}")).
          // map(row => (row.getInt(0), row.getString(2))).
          reduceByKey((c1, c2) => s"$c1\n$c2").
          map(row => (row._1, Row(row._2)))
      }
      else {
        bugsLongdescsDataFrame.
          map(row => (row.getInt(0), s"t${row.getTimestamp(1).getTime}:: ${row.getString(2)}")).
          // map(row => (row.getInt(0), row.getString(2))).
          groupByKey().
          map(row => (row._1, Row(row._2.head)))
      }

    val bugAssignmentData = bugAssignmentDataFrame.
      map(row => BugAssignmentData(row.getInt(0), row.getLong(1)))

    val assignments = bugAssignmentData.collect.
      zipWithIndex.map(elem => elem._1.assigned_to -> elem._2).toMap

    // join dataframes
    val bugsRDD = idAndDescRDD.leftOuterJoin[Row](bugsLongdescsRDD).
      map { row =>
        val key = row._1
        val r1 = row._2._1
        val r2Opt = row._2._2
        r2Opt match {
          case Some(r2) =>
            BugData(
              key,
              s"${r1.getString(0)}\n${r2.getString(0)}",
              r1.getString(1),
              r1.getInt(2),
              assignments.getOrElse(r1.getInt(2), -1).toDouble,
              r1.getInt(3),
              r1.getString(4)
            )
          case None =>
            BugData(
              key,
              r1.getString(0),
              r1.getString(1),
              r1.getInt(2),
              assignments.getOrElse(r1.getInt(2), -1).toDouble,
              r1.getInt(3),
              r1.getString(4)
            )
        }
      }

    bugsRDD
  }
}

case class BugData(bug_id: Integer, bug_data: String,
                   bug_status: String, assigned_to: Integer,
                   assignment_class: Double,
                   component_id: Integer, bug_severity: String)

case class BugAssignmentData(assigned_to: Integer, no: Long)

