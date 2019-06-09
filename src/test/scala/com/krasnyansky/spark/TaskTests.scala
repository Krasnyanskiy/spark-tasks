package com.krasnyansky.spark

import java.lang.Integer.parseInt

import com.krasnyansky.spark.Tasks.{enrichInputWithUserSession, findMedianSessionDuration, findTopRankedProducts, findUniqueUsers}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import spark.implicits._

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

  test("enrichInputWithUserSession returns result with proper amount of rows") {
    val enrichedInput = enrichInputWithUserSession(events)
    assert(enrichedInput.count() === 27)
  }

  test("findUniqueUsers returns result with proper number of rows") {
    val uniqueUsers = findUniqueUsers(enrichInputWithUserSession(events))
    assert(uniqueUsers.count() === 3)
  }

  test("findUniqueUsers returns proper number of users that spend less than 1 minute on product pages of 'Notebooks' category") {
    val uniqueUsers = findUniqueUsers(enrichInputWithUserSession(events))
    val usersNumber = getValue(uniqueUsers, "notebooks", "lessThanOneMin")
    assert(uniqueUsers.count() === 3)
    assert(usersNumber === 1)
  }

  test("findMedianSessionDuration returns result with proper number of rows") {
    val mediaSessionDuration = findMedianSessionDuration(enrichInputWithUserSession(events))
    assert(mediaSessionDuration.count() === 3)
  }

  test("findMedianSessionDuration returns result with proper median for 'Books' category") {
    val mediaSessionDuration = findMedianSessionDuration(enrichInputWithUserSession(events))
    val median = getValue(mediaSessionDuration, "books", "median")
    assert(median === 373)
  }

  test("findRankedProducts returns result with proper number of rows") {
    val topRankedProducts = findTopRankedProducts(events, 3)
    assert(topRankedProducts.count() === 9)
  }

  test("findRankedProducts returns result with proper session duration") {
    val topRankedProducts = findTopRankedProducts(events, 3)
    val topSessionDurationByBooksCategory = getRankedValue(topRankedProducts, "books", "duration", rank = 1)
    assert(topSessionDurationByBooksCategory === 110)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  private def getValue(df: DataFrame, category: String, column: String) =
    df where 'category === category select column collect() map {
      case Row(v) => parseInt(v.toString)
    } head

  private def getRankedValue(df: DataFrame, category: String, c1: String, rank: Int) =
    df where 'category === category && 'rank === rank select c1 collect() map {
      case Row(v) => parseInt(v.toString)
    } head


}
