package dr.acf.recc

import dr.acf.connectors.MySQLConnector
import dr.acf.spark.SparkOps

/**
  * The main algorithm behind the Reccomender System
  * Created by aflorea on 18.11.2015.
  */
object ReccomenderBackbone extends SparkOps with MySQLConnector {

  def main(args: Array[String]) {

    // Step 1 - load data from DB

    // Main bug data
    val bugsDataFrame = mySQLDF(
      "(" +
        "select b.bug_id, b.assigned_to, b.bug_status, b.short_desc, b.component_id, " +
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
        "select l.bug_id, l.thetext " +
        "from longdescs l " +
        ") as buglongdescsslice"
    )

    println(bugsDataFrame.count())
    println(bugsDuplicatesDataFrame.count())
    println(bugsLongdescsDataFrame.count())

    // Step 2 - extract features

    // Step 3...Infinity - TDB

  }
}
