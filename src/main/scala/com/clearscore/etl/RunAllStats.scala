package com.clearscore.etl

import com.clearscore.spark.SparkScriptRunner
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object RunAllStats extends SparkScriptRunner {

  val name = "RunAllStats"

  override def run(spark: SparkSession, logger: Logger, config: Config): Unit = {

    // Import and initially clean input data, removing corrupt records, get latest reports for each user
    ImportAccounts.run(spark, logger, config)
    ImportReports.run(spark, logger, config)
    GetLatestReport.run(spark, logger, config)

    // Simple stats run on imported parquet data
    AverageCreditScore.run(spark, logger, config)
    EmploymentStatus.run(spark, logger, config)

    // Stats and user summaries using most recent reports
    ScoreBucketing.run(spark, logger, config)
    UserSummaries.run(spark, logger, config)

  }

}
