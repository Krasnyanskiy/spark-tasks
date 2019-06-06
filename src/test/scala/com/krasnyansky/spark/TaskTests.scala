package com.krasnyansky.spark

import java.lang.Integer.parseInt

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

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

  test("`enrichDatasetWithUserSession` function returns proper amount of rows (27)") {
    val df = Tasks.enrichDatasetWithUserSession(events)
    assert(df.count() === 27)
  }

  test("`findUniqueUsers` function returns a proper amount of rows (3) and `LessThanOneMin` for `Books` category has proper value (2)") {
    val df = Tasks.findUniqueUsers(Tasks.enrichDatasetWithUserSession(events))

    assert(df.count() === 3)
    assert(getSingleValue(df, "lessThanOneMin") === 2)
  }

  test("`findMedianSessionDuration` function returns proper amount of rows (3) and median for `Books` category has proper value (25)") {
    val df = Tasks.findMedianSessionDuration(Tasks.enrichDatasetWithUserSession(events))

    assert(df.count() === 3)
    assert(getSingleValue(df, "median") === 25)
  }

  test("`findRankedProducts` function returns proper amount of rows (3) and checks that top ranked book is `Scala for Dummies`") {
    val df = Tasks.findTopRankedProducts(events)

    assert(df.count() === 3)
    assert(getFirstOfTop10Books(df) === "Scala for Dummies")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  // ======================================== Helpers ========================================

  private def getSingleValue(df: DataFrame, col: String) = {
    import spark.implicits._
    df.where($"category" === "books").select(col).collect().map { case Row(median) => parseInt(median.toString) }.head
  }

  private def getFirstOfTop10Books(df: DataFrame) = {
    import spark.implicits._
    df.where($"category" === "books").select("top10ProductsSortedByDuration").collect().map { case Row(xs) => xs }
      .head.asInstanceOf[mutable.WrappedArray.ofRef[String]].head
  }

}
