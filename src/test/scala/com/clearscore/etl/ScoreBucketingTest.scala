package com.clearscore.etl

import org.scalatest.{FlatSpec, Matchers}
import com.clearscore.etl.ScoreBucketing.formatNormalisedScore

class ScoreBucketingTest extends FlatSpec with Matchers {

  "formatNormalisedScore" should "correctly format a given integer input for the score range it represents" in {
    formatNormalisedScore(2, 50) shouldBe "101.0-150.0"
  }

  it should "correctly format a given integer for a non 50 score range" in {
    formatNormalisedScore(3, 30) shouldBe "91.0-120.0"
  }

  it should "correctly format a 0 score input" in {
    formatNormalisedScore(0, 20) shouldBe "0.0-20.0"
  }

  // TODO: consider behaviour given a negative input (although we should never have a negative credit score)

}
