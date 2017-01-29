name := "columbugus"

version := "2.3"

scalaVersion := "2.10.6"

// The Typesafe repository
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "Maven2 repository" at "http://repo1.maven.org/maven2/"
resolvers += "Maven repository" at "http://mvnrepository.com/artifact/"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.6.3" //% "provided"
  , "org.apache.spark" %% "spark-sql" % "1.6.3" //% "provided"
  , "org.apache.spark" %% "spark-mllib" % "1.6.3" //% "provided"
  , "com.databricks" %% "spark-csv" % "1.4.0"
  , "org.apache.hadoop" % "hadoop-common" % "2.5.2" % "provided" excludeAll ExclusionRule(organization = "javax.servlet")
  , "org.apache.hadoop" % "hadoop-hdfs" % "2.5.2" % "provided" excludeAll ExclusionRule(organization = "javax.servlet")
  , "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.6.1"
  , "com.typesafe" % "config" % "1.3.0"
  , "mysql" % "mysql-connector-java" % "5.1.37"
  , "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2"
  , "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" classifier "models"
  , "org.slf4j" % "slf4j-api" % "1.7.13"
  , "com.typesafe.slick" %% "slick" % "3.1.1"
  , "tw.edu.ntu.csie" % "libsvm" % "3.17"

)