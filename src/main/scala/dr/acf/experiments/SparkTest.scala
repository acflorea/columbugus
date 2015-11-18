package dr.acf.experiments

import dr.acf.connectors.MySQLConnector
import dr.acf.spark.SparkOps

/**
  * Created by aflorea on 15.11.2015.
  */
object SparkTest extends SparkOps with MySQLConnector {
  def main(args: Array[String]) {
    // Should be some file on the system
    val logFile = "./src/main/resources/dummy-test-files/SPARK-README.md"
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    // MySQL tests
    val jdbcDF = mySQLDF("(select bug_id, assigned_to, bug_status from bugs) as bugslice")
    jdbcDF.collect() foreach println

  }
}
