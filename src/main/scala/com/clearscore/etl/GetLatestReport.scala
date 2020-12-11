package com.clearscore.etl

import com.clearscore.spark.SparkScriptRunner
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, desc, to_timestamp}

object GetLatestReport extends SparkScriptRunner {
  val name = "GetLatestReport"

  override def run(spark: SparkSession, logger: Logger, config: Config): Unit = {
    import spark.implicits._
    val reportsPath = config.getString("reports.reports_unified")
    val outputPath = config.getString("reports.latest_reports")
    val reportsDf = spark.read.parquet(reportsPath)
      .withColumn("pulled-timestamp", to_timestamp($"pulled-timestamp"))

    val window = Window.partitionBy("user-uuid").orderBy(desc("pulled-timestamp"))
    val latest = reportsDf.withColumn("row",row_number.over(window))
      .where($"row" === 1).drop("row")

    latest.write.mode("overwrite").parquet(outputPath)
  }
}
