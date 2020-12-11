name := "clearscore"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.14.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.14.0",
  "com.typesafe" % "config" % "1.2.1",
  "com.concurrentthought.cla" %% "command-line-arguments" % "0.5.0",
  "com.concurrentthought.cla" %% "command-line-arguments-examples" % "0.5.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}