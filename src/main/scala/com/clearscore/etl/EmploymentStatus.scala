package com.clearscore.etl

import com.clearscore.spark.SparkScriptRunner
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{size, collect_set}

object EmploymentStatus extends SparkScriptRunner {
  val name = "EmploymentStatus"

  override def run(spark: SparkSession, logger: Logger, config: Config): Unit = {
    val accountsPath = config.getString("accounts.accounts_parquet")
    val employmentStatusPath = config.getString("stats.employment_status")
    val accountsDF = spark.read.parquet(accountsPath)

    val employmentStatusAndId = accountsDF.select("accountId", "account.user.employmentStatus")
    val grouped = employmentStatusAndId.groupBy("employmentStatus")
      .agg(size(collect_set("accountId")).as("count"))
    // this give the distinct number of accounts for each employment status - rather than just the number of records,
    // allowing for duplicates in the data

    grouped.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true").csv(employmentStatusPath)
  }
}
