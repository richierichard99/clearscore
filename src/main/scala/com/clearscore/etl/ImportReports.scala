package com.clearscore.etl

import com.clearscore.spark.SparkScriptRunner
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.nio.file.{FileSystems, Files}
import java.io.File
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.explode
import org.apache.commons.io.FileUtils.copyFile

object ImportReports extends SparkScriptRunner {

  val name = "ImportReports"

  def readJsonReports(path: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val rawDf = spark.read.json(path)
    rawDf
      .filter($"_corrupt_record".isNull)
      .select(
      $"user-uuid",
      $"account-id",
      $"pulled-timestamp",
      explode($"report.ScoreBlock.Delphi.Score").as("Score"),
      $"report.Summary.Payment_Profiles.CPA.Bank"
    )
  }

  override def run(spark: SparkSession, logger: Logger, config: Config): Unit = {
    implicit val ss: SparkSession = spark

    val reportsInputPath = config.getString("reports.raw_reports")
    val reportsOutputPath = config.getString("reports.reports_unified")
    val reportsDir = FileSystems.getDefault.getPath(reportsInputPath)
    // finds all files in a directory & subdirectories and copy to merged directory
    val jsonFiles = Files.walk(reportsDir).iterator().asScala.filter(Files.isRegularFile(_))

    jsonFiles.foreach(path =>
      copyFile(path.toFile, new File(s"$reportsInputPath-merged/${path.getFileName}"))
    )

    val reportsDf = readJsonReports(s"$reportsInputPath-merged/")
    reportsDf.write.mode("overwrite").parquet(reportsOutputPath)
  }
}
