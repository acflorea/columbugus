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

  implicit lazy val sc = {
    val master = conf.getString("spark.master")
    val appName = conf.getString("spark.appName")

    val sparkConf = new SparkConf().setAppName(appName)
    //.setMaster(master).setAppName(appName)

    sparkConf.set("spark.driver.memory", conf.getString("spark.driver.memory"))
    sparkConf.set("spark.driver.maxResultSize", conf.getString("spark.driver.maxResultSize"))


    new SparkContext(sparkConf)
  }

  lazy val sqlContext = SparkSession.builder().getOrCreate()

}
