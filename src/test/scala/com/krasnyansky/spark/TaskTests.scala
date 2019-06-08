package com.krasnyansky.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TaskTests extends FunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder().appName("SparkTasks Testings").master("local").getOrCreate()
  var events: DataFrame = _

  override def beforeAll(): Unit = {
    events = spark.read
      .option("delimiter", "\t")
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .option("inferSchema", "true")
      .csv("src/test/resources/data/test_events.csv")
  }

  test("`enrichInputWithUserSession` function returns proper amount of rows (27)") {
    val df = Tasks.enrichInputWithUserSession(events)
    assert(df.count() === 27)
  }

  test("`findUniqueUsers` function") {
    ???
  }

  test("`findMedianSessionDuration` function") {
    ???
  }

  test("`findRankedProducts` function returns proper amount of rows (3) and checks that top ranked book is `Scala for Dummies`") {
    ???
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}
