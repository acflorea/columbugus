package dr.acf.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by aflorea on 17.11.2015.
  */
trait SparkOps

object SparkOps {

  val conf = ConfigFactory.load()

  implicit lazy val sc = {
    val master = conf.getString("spark.master")
    val appName = conf.getString("spark.appName")

    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)

    sparkConf.set("spark.driver.memory", conf.getString("spark.driver.memory"))
    sparkConf.set("spark.driver.maxResultSize", conf.getString("spark.driver.maxResultSize"))


    new SparkContext(sparkConf)
  }

  lazy val sqlContext = new org.apache.spark.sql.SQLContext(sc)

}
