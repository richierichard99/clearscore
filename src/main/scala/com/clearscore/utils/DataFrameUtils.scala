package com.clearscore.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, explode}

object DataFrameUtils {

  def extractScoreBlock: Column = {
    explode(col("Delphi.Score")).cast("double").as("Score")
  }
}
