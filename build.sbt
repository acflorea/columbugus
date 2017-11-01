
name := "columbugus"

version := "2.3.2"

packAutoSettings

scalaVersion := "2.11.11"
autoScalaLibrary := false

val sparkVersion = "2.1.1"
val hadoopVersion = "2.8.0"

val sparkDependenciesScope = "provided"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

// The Typesafe repository
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "Maven2 repository" at "http://repo1.maven.org/maven2/"
resolvers += "Maven repository" at "http://mvnrepository.com/artifact/"

val sparkExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.apache.hadoop", "hadoop-client").
    exclude("org.apache.hadoop", "hadoop-yarn-client").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.apache.hadoop", "hadoop-yarn-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.esotericsoftware.minlog", "minlog")

val hadoopClientExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.slf4j", "slf4j-api").
    exclude("javax.servlet", "servlet-api")

// aspark libraries
lazy val sparkDependencies = Seq(
  sparkExcludes("org.apache.spark" %% "spark-core" % sparkVersion),
  sparkExcludes("org.apache.spark" %% "spark-yarn" % sparkVersion),
  sparkExcludes("org.apache.spark" %% "spark-sql" % sparkVersion),
  sparkExcludes("org.apache.spark" %% "spark-hive" % sparkVersion),
  sparkExcludes("org.apache.spark" %% "spark-streaming" % sparkVersion),
  sparkExcludes("org.apache.spark" %% "spark-mllib" % sparkVersion),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-applications-distributedshell" % hadoopVersion),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % hadoopVersion),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion),
  "org.apache.commons" % "commons-lang3" % "3.5"
)

// additional libraries
lazy val assemblyDependencies = Seq(
  "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.6.1"
  , "com.typesafe" % "config" % "1.3.0"
  , "mysql" % "mysql-connector-java" % "5.1.37"
  , "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2"
  , "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" classifier "models"
  , "org.slf4j" % "slf4j-api" % "1.7.13"
  , "com.typesafe.slick" %% "slick" % "3.1.1"
  , "tw.edu.ntu.csie" % "libsvm" % "3.17"
)

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= sparkDependencies.map(_ % sparkDependenciesScope) ++ assemblyDependencies

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "unwanted.txt" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

lazy val mainRunner = project.in(file("mainRunner")).dependsOn(RootProject(file("."))).settings(
  assembly := new File(""),
  scalaVersion := "2.11.11",
  libraryDependencies ++= sparkDependencies.map(_ % "compile") ++ assemblyDependencies
)