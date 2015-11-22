package dr.acf.recc

import com.typesafe.config.ConfigFactory
import dr.acf.connectors.MySQLConnector
import dr.acf.spark.SparkOps
import org.apache.spark.ml.feature.{RegexTokenizer, IDF, HashingTF, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * The main algorithm behind the Reccomender System
  * Created by aflorea on 18.11.2015.
  */
object ReccomenderBackbone extends SparkOps with MySQLConnector {

  def main(args: Array[String]) {

    // Step 1 - load data from DB

    val bugInfoRDD: RDD[BugData] = buildBugsRDD

    import sqlContext.implicits._
    val bugInfoDF = bugInfoRDD.toDF()

    val tokenizer = new RegexTokenizer("\\w+|\\$[\\d\\.]+|\\S+").
      setMinTokenLength(3).setInputCol("bug_data").setOutputCol("words")
    val wordsData = tokenizer.transform(bugInfoDF)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features", "words", "bug_id").take(3).foreach(println)

    val stopHere = true

    // Step 2 - extract features

    // Step 3...Infinity - TDB

  }

  private lazy val testMode: Boolean = {
    val conf = ConfigFactory.load()
    conf.getBoolean("global.testMode")
  }

  /**
    * Returns an RDD containing (bug_id, bug_details) type of data
    * The details are in the form t<timestamp>:: info
    * (the description and comments are included)
    * @return
    */
  def buildBugsRDD: RDD[BugData] = {

    val extraFilter =
      (column: String) => if (testMode) s"$column > 10000 and $column < 20000 " else "1 = 1 "

    // Main bug data
    val bugsDataFrame = mySQLDF(
      "(" +
        "select b.bug_id, b.creation_ts, b.short_desc," +
        "b.bug_status, b.assigned_to, b.component_id, b.bug_severity, " +
        "c.name as component_name, c.product_id, " +
        "p.name as product_name, p.classification_id, " +
        "cl.name as classification_name " +
        "from bugs b " +
        "join components c on b.component_id = c.id " +
        "join products p on c.product_id = p.id " +
        "join classifications cl on p.classification_id = cl.id " +
        "where " + extraFilter("b.bug_id") +
        ") as bugslice"
    )

    // Duplicates -- Which bugs are duplicates of which other bugs.
    val bugsDuplicatesDataFrame = mySQLDF(
      "(" +
        "select d.dupe_of, d.dupe " +
        "from duplicates d " +
        "where " + extraFilter("d.dupe_of") +
        ") as bugduplicates"
    )

    // The meat of bugzilla -- here is where all user comments are stored!
    val bugsLongdescsDataFrame = mySQLDF(
      "(" +
        "select l.bug_id, l.bug_when, l.thetext " +
        "from longdescs l " +
        "where " + extraFilter("l.bug_id") +
        ") as buglongdescsslice"
    )

    // ($bug_id,t$timestamp:: $short_desc)
    val idAndDescRDD = bugsDataFrame.select("bug_id", "creation_ts", "short_desc",
      "bug_status", "assigned_to", "component_id", "bug_severity").
      map(row => (row.getInt(0), Row(s"t${row.getTimestamp(1).getTime}:: ${row.getString(2)}",
        row.getString(3), row.getInt(4), row.getInt(5), row.getString(6))))

    // ($bug_id,(t$timestamp:: $comment)...)
    val bugsLongdescsRDD = bugsLongdescsDataFrame.
      map(row => (row.getInt(0), s"t${row.getTimestamp(1).getTime}:: ${row.getString(2)}")).
      reduceByKey((c1, c2) => s"$c1\n$c2").
      map(row => (row._1, Row(row._2)))

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

    bugsRDD
  }
}

case class BugData(bug_id: Integer, bug_data: String,
                   bug_status: String, assigned_to: Integer,
                   component_id: Integer, bug_severity: String)

