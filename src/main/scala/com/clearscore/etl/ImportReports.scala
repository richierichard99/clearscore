package com.clearscore.etl

import com.clearscore.spark.SparkScriptRunner
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.nio.file.{FileSystems, Files}
import scala.collection.JavaConverters._

object ImportReports extends SparkScriptRunner {

  val name = "ImportReports"

  def checkJsonIsCorrupt(jsonDF: DataFrame): Boolean = {
    jsonDF.columns.contains("_corrupt_record")
  }

  def readJsonReports(path: String)(implicit spark: SparkSession): Option[DataFrame] = {
    val rawDf = spark.read.json(path)
    if (checkJsonIsCorrupt(rawDf)) None
    else Some(rawDf.select(
      "user-uuid",
      "account-id",
      "pulled-timestamp",
      "report.ScoreBlock.Delphi",
      "report.Summary.Payment_Profiles.CPA.Bank"
    ))
  }

  override def run(spark: SparkSession, logger: Logger, config: Config): Unit = {
    implicit val ss: SparkSession = spark

    val reportsInputPath = config.getString("reports.raw_reports")
    val reportsOutputPath = config.getString("reports.reports_unified")
    val reportsDir = FileSystems.getDefault.getPath(reportsInputPath)
    // finds all files in a directory & subdirectories
    // this may not work so well on a hdfs system for example
    // a function to differentiate between file systems and find the json files specifically for each one would be needed
    // for this to be truly scalable
    val jsonFiles = Files.walk(reportsDir).iterator().asScala.filter(Files.isRegularFile(_))

    // I'm not very happy with this - it works but it's really memory intensive,
    // with more time I'd have liked to it better
    jsonFiles.flatMap(file => readJsonReports(file.toString))
      .reduce(_ union _)
      .write.mode("overwrite").parquet(reportsOutputPath)
  }
}
