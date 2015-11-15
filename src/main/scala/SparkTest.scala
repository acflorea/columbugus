import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by aflorea on 15.11.2015.
  */
object SparkTest {
  def main(args: Array[String]) {
    val logFile = "./src/main/resources/dummy-test-files/SPARK-README.md" // Should be some file on your system
    val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
