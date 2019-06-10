package com.krasnyansky.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

object Tasks {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val events = spark.read
      .option("delimiter", "\t")
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/events.csv")

    val enrichedInputWithUserSession = enrichInputWithUserSession(events)
    val medianSessionDuration = findMedianSessionDuration(enrichedInputWithUserSession)
    val uniqueUsers = findUniqueUsers(enrichedInputWithUserSession)
    val topRankedProducts = findTopRankedProducts(events, 10)

    printDFs(enrichedInputWithUserSession, medianSessionDuration, uniqueUsers, topRankedProducts)
  }

  /**
    * Enriches input (events) with user sessions.  By session we mean consecutive events that belong
    * to a single category and aren't more than 5 minutes away from each other.
    *
    * @return new DF with user session
    */
  def enrichInputWithUserSession(events: DataFrame): DataFrame = {
    val eventWindow = Window.partitionBy("category", "userId").orderBy("eventTime")
    val sessionIdWindow = Window.partitionBy("sessionId")
    val uniqueSession = Window.orderBy("uniqueSession")

    val `5 minutes` = 5 * 60

    events
      .withColumn("sessionChanged", sum(coalesce(unix_timestamp('eventTime) - lag(unix_timestamp('eventTime), 1).over(eventWindow), lit(0)) > `5 minutes` cast "int").over(eventWindow))
      .withColumn("uniqueSession", concat_ws("+", 'category, 'userId, 'sessionChanged))
      .withColumn("sessionId", rank.over(uniqueSession)) // make readable session id
      .withColumn("sessionStartTime", first("eventTime").over(sessionIdWindow))
      .withColumn("sessionEndTime", last("eventTime").over(sessionIdWindow))
      .drop("sessionChanged", "uniqueSession")
  }

  /**
    * Finds median session duration.
    *
    * @return DF that contains median session duration (statistics)
    */
  def findMedianSessionDuration(eventsWithUserSessions: DataFrame): DataFrame = {
    eventsWithUserSessions
      .withColumn("duration", unix_timestamp('sessionEndTime) - unix_timestamp('sessionStartTime))
      .groupBy("category")
      .agg(callUDF("percentile_approx", col("duration"), lit(0.5)) as "median")
  }

  /**
    * Finds number of unique users spending less than 1 minute, 1 to 5 minutes and more than 5 minutes
    * for each category.
    *
    * @return DF with unique users stats
    */
  def findUniqueUsers(sessionStatistic: DataFrame): DataFrame = {
    val withDuration = sessionStatistic
      .withColumn("duration", (unix_timestamp('sessionEndTime) - unix_timestamp('sessionStartTime))/60)

    withDuration
      .groupBy("category")
      .agg(
        countDistinct('userId, when('duration < 1, 'userId)) as "lessThanOneMin",
        countDistinct('userId, when('duration > 1 && 'duration < 5, 'userId)) as "oneToFiveMins",
        countDistinct('userId, when('duration > 5, 'userId)) as "moreThanFiveMins"
      )
  }

  /**
    * Finds top 10 products ranked by time spent by users on product pages for each category.
    */
  def findTopRankedProducts(events: DataFrame, topProductLimit: Int): DataFrame = {
    val eventWindow = Window.partitionBy('category, 'userId).orderBy('eventTime)
    val productWindow = Window.partitionBy('product).orderBy('eventTime)
    val sessionWindow = Window.partitionBy('session).orderBy('product)
    val sessionIdWindow = Window.orderBy('session)
    val categoryWindow = Window.partitionBy('category).orderBy('duration.desc)

    events
      .withColumn("e", rank.over(eventWindow))
      .withColumn("p", rank.over(productWindow))
      .withColumn("session", base64(concat_ws("+", 'category, 'userId, 'e - 'p, 'product)))
      .withColumn("difference", coalesce(unix_timestamp('eventTime) - lag(unix_timestamp('eventTime), 1).over(sessionWindow), lit(0)))
      .withColumn("sessionId", rank.over(sessionIdWindow))
      .groupBy('category, 'product, 'userId, 'sessionId).agg(sum('difference) as "duration")
      .withColumn("rank", rank.over(categoryWindow))
      .where('rank <= topProductLimit)
  }

  /**
    * Prints DF with a limit - maximum 1 million rows.
    */
  def printDFs(df: DataFrame*): Unit = df.foreach(_.show(1000000, truncate = false))
}