package dr.acf.connectors

import java.sql.DriverManager

import com.typesafe.config.ConfigFactory
import dr.acf.spark.SparkOps
import org.apache.spark.sql.DataFrame

/**
  * Created by aflorea on 17.11.2015.
  */
trait MySQLConnector {

  this: SparkOps =>

  def mySQLdf(query: String): DataFrame = {
    // MySQL test
    Class.forName("com.mysql.jdbc.Driver")
    val conf = ConfigFactory.load()
    val url = conf.getString("mySQL.url")
    val username = conf.getString("mySQL.username")
    val password = conf.getString("mySQL.password")
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> (url + "?user=" + username + "&password=" + password),
        "dbtable" -> query)
    ).load()
    jdbcDF
  }

}
