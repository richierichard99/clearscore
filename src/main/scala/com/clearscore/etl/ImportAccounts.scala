package com.clearscore.etl

import com.clearscore.spark.SparkScriptRunner
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object ImportAccounts extends SparkScriptRunner {

  val name = "ImportAccounts"

  override def run(spark: SparkSession, logger: Logger, config: Config): Unit = {
    import spark.implicits._

    val accountsRawPath = config.getString("accounts.raw_accounts")
    val accountsParquetPath = config.getString("accounts.accounts_parquet")
    val accountsDF = spark.read.json(accountsRawPath)

    accountsDF.filter($"accountId".isNotNull && $"account".isNotNull)
      .drop("_corrupt_record")
      .write.mode("overwrite").parquet(accountsParquetPath)
  }
}
