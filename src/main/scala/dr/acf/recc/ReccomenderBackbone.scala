package dr.acf.recc

import com.typesafe.config.ConfigFactory
import dr.acf.connectors.MySQLConnector
import dr.acf.spark.{SVMWithSGDMulticlass, SparkOps}
import org.apache.spark.ml.feature.{Tokenizer, HashingTF, IDF, RegexTokenizer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

/**
  * The main algorithm behind the Reccomender System
  * Created by aflorea on 18.11.2015.
  */
object ReccomenderBackbone extends SparkOps with MySQLConnector {

  def logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]) {

    // Step 1 - load data from DB

    logger.debug("Start!")

    val (bugInfoRDD: RDD[BugData], bugsAssignmentRDD: RDD[BugAssignmentData]) = buildBugsRDD
    import sqlContext.implicits._
    val bugInfoDF = bugInfoRDD.toDF()

    val assignments = bugsAssignmentRDD.collect.
      zipWithIndex.map(elem => elem._1.assigned_to -> elem._2).toMap

    // Step 2 - extract features
    val tokenizer = if (tokenizerType == 0)
      new Tokenizer().setInputCol("bug_data").setOutputCol("words")
    else
      new RegexTokenizer("\\w+|\\$[\\d\\.]+|\\S+").
        setMinTokenLength(minWordSize).setInputCol("bug_data").setOutputCol("words")

    val wordsData = tokenizer.transform(bugInfoDF)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
    //.setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    // Step 2.a :) dummy classifier
    val labeledPoints = rescaledData.select("features", "assigned_to").
      map { point =>
        val features = point.getAs[Vector]("features")
        val assigned_to = assignments.get(point.getAs[Integer]("assigned_to")) match {
          case Some(index) => index
          case None => assignments.size + 1
        }
        LabeledPoint(assigned_to, features)
      }

    // Split data into training (60%) and test (40%).
    val splits = labeledPoints.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    // val numIterations = 20
    //    val model = LogisticRegressionWithSGD.train(training, numIterations)

    //    // Run training algorithm to build the model
    //    val model = new LogisticRegressionWithLBFGS()
    //      .setNumClasses(assignments.size)
    //      .run(training)

    val model = new SVMWithSGDMulticlass().train(training, 250, 1, 0.01, 1)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
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

  private lazy val (testMode, includeComments, tokenizerType, minWordSize) = {
    val conf = ConfigFactory.load()
    (
      conf.getBoolean("global.testMode"),
      conf.getBoolean("global.includeComments"),
      conf.getInt("global.tokenizerType"),
      conf.getInt("global.minWordSize")
      )
  }

  /**
    * Returns an RDD containing (bug_id, bug_details) type of data
    * The details are in the form t<timestamp>:: info
    * (the description and comments are included)
    * @return
    */
  def buildBugsRDD: (RDD[BugData], RDD[BugAssignmentData]) = {

    val testFilter =
      (column: String) => if (testMode) s"$column > 318000 " else "1 = 1 "

    val resolutionFilter = "resolution = 'FIXED'"

    val dateFilter = "delta_ts = 'FIXED'"

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
        ") as bugslice"
    ).filter(resolutionFilter)

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

    // Assignment data
    val bugAssignmentDataFrame = mySQLDF(
      "(" +
        "select b.assigned_to, count(*) as bugs_assigned " +
        "from bugs b " +
        "where " + testFilter("b.bug_id") +
        "group by b.assigned_to order by bugs_assigned desc" +
        ") as bugslice"
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
          reduceByKey((c1, c2) => s"$c1\n$c2").
          map(row => (row._1, Row(row._2)))
      }
      else {
        bugsLongdescsDataFrame.
          map(row => (row.getInt(0), s"t${row.getTimestamp(1).getTime}:: ${row.getString(2)}")).
          groupByKey().
          map(row => (row._1, Row(row._2.head)))
      }

    val bugAssignmentData = bugAssignmentDataFrame.
      map(row => BugAssignmentData(row.getInt(0), row.getLong(1)))

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
              r1.getString(0) + r2.getString(0),
              r1.getString(1),
              r1.getInt(2),
              r1.getInt(3),
              r1.getString(4)
            )
          case None =>
            BugData(
              key,
              r1.getString(0),
              r1.getString(1),
              r1.getInt(2),
              r1.getInt(3),
              r1.getString(4)
            )
        }
      }

    (bugsRDD, bugAssignmentData)
  }
}

case class BugData(bug_id: Integer, bug_data: String,
                   bug_status: String, assigned_to: Integer,
                   component_id: Integer, bug_severity: String)

case class BugAssignmentData(assigned_to: Integer, no: Long)

