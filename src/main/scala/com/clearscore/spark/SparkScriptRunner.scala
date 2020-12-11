package com.clearscore.spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.concurrentthought.cla._

import java.io.File

trait SparkScriptRunner {
  val name: String
  def run(spark: SparkSession, logger: Logger, config: Config): Unit

  def main(args: Array[String]): Unit = {
    import Opt._
    val initialArgs = Args(
      "run-main SparkScript [options]",
      "runs named spark script using paths from external config file",
      """Note that --config is required.""",
      Seq(
        string("config",     Seq("-c", "--config"))
      )
    )
    val finalArgs: Args = initialArgs.process(args)
    val configPath = finalArgs.get[String]("config")
      .getOrElse(throw new IllegalArgumentException("config flag not defined. use -c or --config"))

    val conifg = ConfigFactory.parseFile(new File(configPath))
    val logger = Logger.getLogger("com.clearscore")
    logger.setLevel(Level.INFO) // todo: make configurable
    val spark = SparkSession.builder.appName(name).getOrCreate()

    logger.info(s"Starting Script: $name")
    run(spark, logger, conifg.resolve)
    spark.close()
  }
}
