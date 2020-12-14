package com.clearscore.etl

import com.clearscore.spark.SparkScriptRunner
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ceil, lit, udf, upper}

object ScoreBucketing extends SparkScriptRunner {
  val name = "ScoreBucketing"

  def formatNormalisedScore(score: Double, range: Double): String = {
    val upperBound = if (score == 0) range else score*range
    val lowerBound = if (upperBound == range) 0.0 else (upperBound - range) + 1
    s"$lowerBound-$upperBound"
  }

  private val formatNormalisedScoreUdf = udf((x: Double, range: Double) => formatNormalisedScore(x, range))

  override def run(spark: SparkSession, logger: Logger, config: Config): Unit = {
    import spark.implicits._

    val reportsPath = config.getString("reports.latest_reports")
    val outputPath = config.getString("stats.score_range_output")

    val scoreRange = config.getDouble("stats.score_range")

    val reportsDf = spark.read.parquet(reportsPath)
    val idsAndScores = reportsDf
      .select($"user-uuid", $"Score")
      .withColumn("normalised_rounded", ceil($"Score"/scoreRange))

    val groupedAndFormatted = idsAndScores.groupBy("normalised_rounded").count()
      .withColumn("score-range", formatNormalisedScoreUdf($"normalised_rounded", lit(scoreRange)))

    groupedAndFormatted.coalesce(1)
      .write.mode("overwrite").option("header", "true").csv(outputPath)
  }

}
