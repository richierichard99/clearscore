package com.clearscore.etl

import com.clearscore.spark.SparkScriptRunner
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.clearscore.utils.DataFrameUtils.extractScoreBlock

object UserSummaries extends SparkScriptRunner {

  val name = "UserSummaries"

  override def run(spark: SparkSession, logger: Logger, config: Config): Unit = {

    import spark.implicits._

    val accountsPath = config.getString("accounts.accounts_parquet")
    val latestReportsPath = config.getString("reports.latest_reports")

    val userSummaryOutput = config.getString("stats.user_summaries")

    val accounts = spark.read.parquet(accountsPath)
      .select($"uuid".as("user-uuid"), $"accountId", $"account.user.employmentStatus", $"account.user.bankName")

    val reports = spark.read.parquet(latestReportsPath)
      .select(
        $"user-uuid",
        $"accountId",
        extractScoreBlock,
        $"Bank.Total_number_of_Bank_Active_accounts_",
        $"Bank.Total_outstanding_balance_on_Bank_active_accounts"
      )

    val joined = reports.join(accounts, Seq("user-uuid"), "left_outer")

    joined.write.mode("overwrite").option("header", "true").csv(userSummaryOutput)
  }

}
