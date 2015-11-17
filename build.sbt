name := "columbugus"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.5.2" % "provided"
  , "org.apache.spark" %% "spark-sql" % "1.5.2"
  , "net.sourceforge.htmlcleaner" %% "htmlcleaner" % "2.6.1"
  , "com.typesafe" %% "config" % "1.3.0"
  , "mysql" %% "mysql-connector-java" % "5.1.37"
)
