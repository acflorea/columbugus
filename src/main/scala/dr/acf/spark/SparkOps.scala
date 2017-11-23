package dr.acf.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by aflorea on 17.11.2015.
  */
trait SparkOps

object SparkOps {

  val conf = ConfigFactory.load().getConfig("reccsys")

  lazy val session = {

    val master = conf.getString("spark.master")
    val appName = conf.getString("spark.appName")

    val spark = SparkSession
      .builder
      .master(master)
      .appName(appName)
      .config("spark.driver.memory", conf.getString("spark.driver.memory"))
      .config("spark.driver.maxResultSize", conf.getString("spark.driver.maxResultSize"))
      .getOrCreate()

    spark
  }

  implicit lazy val sc = session.sparkContext
  implicit lazy val sqlContext = session.sqlContext

}
