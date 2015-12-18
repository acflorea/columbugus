name := "columbugus"

version := "1.0"

scalaVersion := "2.11.7"

// The Typesafe repository
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "Maven2 repository" at "http://repo1.maven.org/maven2/"
// resolvers += "Maven repository" at "http://mvnrepository.com/artifact/"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.5.2" % "provided"
  , "org.apache.spark" %% "spark-sql" % "1.5.2"
  , "org.apache.spark" %% "spark-mllib" % "1.5.2"
  , "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.6.1"
  , "com.typesafe" % "config" % "1.3.0"
  , "mysql" % "mysql-connector-java" % "5.1.37"
  , "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2"
  , "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" classifier "models"
//  , "edu.washington.cs.knowitall.stanford-corenlp" % "stanford-postag-models" % "1.3.5"
)