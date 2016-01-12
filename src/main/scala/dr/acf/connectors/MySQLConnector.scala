package dr.acf.connectors

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import dr.acf.spark.SparkOps
import org.apache.spark.sql.DataFrame

/**
  * Created by aflorea on 17.11.2015.
  */
trait MySQLConnector {

  this: SparkOps =>

  private lazy val (fullURL, url, username, password) = {
    DriverManager.registerDriver(new com.mysql.jdbc.Driver)
    val conf = ConfigFactory.load()
    val url = conf.getString("mySQL.url")
    val username = conf.getString("mySQL.username")
    val password = conf.getString("mySQL.password")
    (url + "?user=" + username + "&password=" + password,
      url,
      username,
      password)
  }

  /**
    * Writes a dataframe to a DB table
    * @param dataframe data to write
    * @param dbtable destination table name
    */
  def writeToTable(dataframe: DataFrame, dbtable: String) = {
    val properties = new Properties()
    properties.put("user", username)
    properties.put("password", password)
    dataframe.write.mode("append").jdbc(url, dbtable, properties)
  }

  /**
    * Returns a Data Frame backed by query
    * @param dbtable - the query behind the dataframe
    * @return a DataFrame wrapper over the database rows
    */
  def mySQLDF(dbtable: String): DataFrame = {
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> fullURL,
        "dbtable" -> dbtable,
        "driver" -> "com.mysql.jdbc.Driver")
    ).load()
    jdbcDF
  }


  /**
    * Returns a Data Frame backed by a query or table name
    * @param dbtable - the query behind the dataframe
    * @param partitionColumn - the column determining the partition
    * @param lowerBound - the minimum value of the first placeholder
    * @param upperBound - the maximum value of the second placeholder
    *                   The lower and upper bounds are inclusive.
    * @param numPartitions - the number of partitions.
    *                      Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
    *                      the query would be executed twice,
    *                      once with (1, 10) and once with (11, 20)
    * @return a DataFrame wrapper over the database rows
    */
  def mySQLDF(dbtable: String,
              partitionColumn: String,
              lowerBound: Int,
              upperBound: Int,
              numPartitions: Int): DataFrame = {

    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> fullURL,
        "dbtable" -> dbtable,
        "driver" -> "com.mysql.jdbc.Driver",
        "partitionColumn" -> partitionColumn,
        "lowerBound" -> lowerBound.toString,
        "upperBound" -> upperBound.toString,
        "numPartitions" -> numPartitions.toString
      )
    ).load()
    jdbcDF
  }

}
