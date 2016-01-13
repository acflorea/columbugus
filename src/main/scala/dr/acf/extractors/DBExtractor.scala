package dr.acf.extractors

import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import dr.acf.connectors.MySQLConnector
import dr.acf.spark.SparkOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Database extractor
  * Created by aflorea on 13.01.2016.
  */
object DBExtractor extends SparkOps with MySQLConnector {

  // cofig stuff
  val conf = ConfigFactory.load()

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


