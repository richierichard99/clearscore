name := "clearscore"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.14.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.14.0",
  "com.typesafe" % "config" % "1.2.1",
  "com.concurrentthought.cla" %% "command-line-arguments" % "0.5.0",
  "com.concurrentthought.cla" %% "command-line-arguments-examples" % "0.5.0"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val buildJars = taskKey[File]("build fat jars for project")

buildJars := {
  val c = (compile in Compile).value
  val t: Unit = (test in Test).value
  assembly.value
}