package com.krasnyansky.spark

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TaskTests extends FunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder().appName("Spark Tasks Testings").master("local").getOrCreate()
  var events: DataFrame = _

  override def beforeAll(): Unit = {
    events = spark.read
      .option("delimiter", "\t")
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .option("inferSchema", "true")
      .csv("src/test/resources/data/test_events.csv")
  }

  test("that `enrichDatasetWithUserSession` function returns proper amount of rows (27)") {
    val df = Tasks.enrichDatasetWithUserSession(events)
    assert(df.count() === 27)
  }

  test("that `findUniqueUsers` function returns a proper amount of rows (3) and `LessThanOneMin` for `Books` category has proper value (2)") {
    import spark.implicits._
    val df = Tasks.findUniqueUsers(Tasks.enrichDatasetWithUserSession(events))
    assert(df.count() === 3)
    assert(df.where($"category" === "books").select("lessThanOneMin").collect().map {
      case Row(lessThanOneMin) => Integer.parseInt(lessThanOneMin.toString)
    }.head === 2)
  }

  test("that `findMedianSessionDuration` function returns proper amount of rows (3) and median for `Books` category has proper value (25)") {
    import spark.implicits._
    val df = Tasks.findMedianSessionDuration(Tasks.enrichDatasetWithUserSession(events))
    assert(df.count() === 3)
    assert(df.where($"category" === "books").select("median").collect().map {
      case Row(median) => Integer.parseInt(median.toString)
    }.head === 25)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}
