package com.clearscore.etl

import org.scalatest.flatspec.AnyFlatSpecLike
import com.clearscore.etl.ScoreBucketing.formatNormalisedScore
import org.scalatest.matchers.should.Matchers

class ScoreBucketingTest extends AnyFlatSpecLike with Matchers {

  "formatNormalisedScore" should "correctly format a given integer input for the score range it represents" in {
    formatNormalisedScore(2, 50) shouldBe "51.0-100.0"
  }

  it should "correctly format a given integer for a non 50 score range" in {
    formatNormalisedScore(3, 30) shouldBe "61.0-90.0"
  }

  it should "correctly format a 0 score input" in {
    formatNormalisedScore(0, 20) shouldBe "0.0-20.0"
  }

  it should "correctly format an input of 1" in {
    formatNormalisedScore(1, 50) shouldBe "0.0-50.0"
  }

  // TODO: consider behaviour given a negative input (although we should never have a negative credit score)

}
