package dr.acf.recc

import dr.acf.connectors.MySQLConnector
import dr.acf.spark.SparkOps
import org.apache.spark.rdd.RDD

/**
  * The main algorithm behind the Reccomender System
  * Created by aflorea on 18.11.2015.
  */
object ReccomenderBackbone extends SparkOps with MySQLConnector {

  def main(args: Array[String]) {

    // Step 1 - load data from DB

    val bugInfoRDD: RDD[(Int, String)] = buildBugsRDD

    bugInfoRDD.take(1).foreach(println)

    val stopHere = true

    // Step 2 - extract features

    // Step 3...Infinity - TDB

  }

  /**
    * Returns an RDD containing (bug_id, bug_details) type of data
    * The details are in the form t<timestamp>:: info
    * (the description and comments are included)
    * @return
    */
  def buildBugsRDD: RDD[(Int, String)] = {
    // Main bug data
    val bugsDataFrame = mySQLDF(
      "(" +
        "select b.bug_id, b.creation_ts, b.assigned_to, " +
        "b.bug_status, b.short_desc, b.component_id, " +
        "c.name as component_name, c.product_id, " +
        "p.name as product_name, p.classification_id, " +
        "cl.name as classification_name " +
        "from bugs b " +
        "join components c on b.component_id = c.id " +
        "join products p on c.product_id = p.id " +
        "join classifications cl on p.classification_id = cl.id " +
        ") as bugslice"
    )

    // Duplicates -- Which bugs are duplicates of which other bugs.
    val bugsDuplicatesDataFrame = mySQLDF(
      "(" +
        "select d.dupe_of, d.dupe " +
        "from duplicates d " +
        ") as bugduplicates"
    )

    // The meat of bugzilla -- here is where all user comments are stored!
    val bugsLongdescsDataFrame = mySQLDF(
      "(" +
        "select l.bug_id, l.bug_when, l.thetext " +
        "from longdescs l " +
        ") as buglongdescsslice"
    )

    // ($bug_id,t$timestamp:: $short_desc)
    val idAndDescRDD = bugsDataFrame.select("bug_id", "creation_ts", "short_desc").
      map(row => (row.getInt(0), s"t${row.getTimestamp(1).getTime}:: ${row.getString(2)}"))

    // ($bug_id,(t$timestamp:: $comment)...)
    val bugsLongdescsRDD = bugsLongdescsDataFrame.
      map(row => (row.getInt(0), s"t${row.getTimestamp(1).getTime}:: ${row.getString(2)}")).
      reduceByKey((c1, c2) => s"$c1\n$c2")


    idAndDescRDD.leftOuterJoin[String](bugsLongdescsRDD).
      map { row =>
        val key = row._1
        val desc = row._2._1
        val commentsOpt = row._2._2
        (key, commentsOpt match {
          case Some(comments) => s"$desc\n$comments"
          case None => desc
        })
      }

  }
}
