package com.clearscore.etl

import com.clearscore.spark.SparkScriptRunner
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, mean}
import com.clearscore.utils.DataFrameUtils.extractScoreBlock

object AverageCreditScore extends SparkScriptRunner {

  val name = "AverageCreditScore"

  override def run(spark: SparkSession, logger: Logger, config: Config): Unit = {
    import spark.implicits._
    val reportsPath = config.getString("reports.reports_unified")
    val outputPath = config.getString("stats.average_score")
    val reportsDf = spark.read.parquet(reportsPath)

    val creditScore = reportsDf
      .select(extractScoreBlock)
      .agg(mean("Score").as("MeanCreditScore"))

    // lovely spark writing everything to partitioned directories
    // in an actual hadoop environment we could use FileUtil.copyMerge to write more reasonable output data
    creditScore.coalesce(1).write.option("header", "true").csv(outputPath)
    // in this case coalesce is pretty safe - we've already aggregated, and we're running in standalone anyway
  }

}
