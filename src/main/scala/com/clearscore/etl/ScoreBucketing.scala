package com.clearscore.etl

import com.clearscore.spark.SparkScriptRunner
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{floor, udf, lit}

object ScoreBucketing extends SparkScriptRunner {
  val name = "ScoreBucketing"

  // TODO: Unit test
  def formatNormalisedScore(score: Double, range: Double): String = {
    val upperBound = (score +1 )*range
    val lowerBound = if (score == 0.0) score else (score*range) + 1
    if (upperBound == 0) "0-50" else s"$lowerBound-$upperBound"
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
      .withColumn("normalised_rounded", floor($"Score"/scoreRange))

    val groupedAndFormatted = idsAndScores.groupBy("normalised_rounded").count()
      .withColumn("score-range", formatNormalisedScoreUdf($"normalised_rounded", lit(scoreRange)))

    groupedAndFormatted.coalesce(1)
      .write.mode("overwrite").option("header", "true").csv(outputPath)
  }

}
