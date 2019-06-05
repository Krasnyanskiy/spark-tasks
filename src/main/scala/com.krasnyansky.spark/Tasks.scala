package com.krasnyansky.spark

import org.apache.spark.sql.DataFrame

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

    // ============================================ Task [1] ============================================

    val enrichedDatasetWithUserSession = enrichDatasetWithUserSession(events)
    printDF(enrichedDatasetWithUserSession)

    // ============================================ Task [2] ============================================

    // [2.1] Get Median

    val medianSessionDuration = findMedianSessionDuration(enrichedDatasetWithUserSession)
    printDF(medianSessionDuration)

    // [2.2] Find number of unique users

    val uniqueUsers = findUniqueUsers(enrichedDatasetWithUserSession)
    printDF(uniqueUsers)

    // [2.3] Find top 10 products

    val rankedProducts = ??? // Todo: Implement me!
  }

  // ============================================= Helpers =============================================

  /**
    * Enriches Dataset (Events) with user sessions.  By session we mean consecutive events that belong
    * to a single category and aren't more than 5 minutes away from each other.
    *
    * @return new DF with user session
    */
  def enrichDatasetWithUserSession(events: DataFrame): DataFrame = {
    val groupedEvents = events.groupBy(window($"eventTime", "5 minutes"), $"eventType")

    groupedEvents.agg(
      min($"eventTime") as "sessionStartTime",
      max($"eventTime") as "sessionEndTime",
      collect_list(array($"category", $"product", $"userId", $"eventTime", $"eventType")) as "record"
    ).toDF()
      .withColumn("sessionId", monotonically_increasing_id())
      .select(explode($"record") as "r", $"sessionId", $"sessionStartTime", $"sessionEndTime")
      .select(
        $"r" (0) as "category",
        $"r" (1) as "product",
        $"r" (2) as "userId",
        $"r" (3) as "eventTime",
        $"r" (4) as "eventType",
        $"sessionId",
        $"sessionStartTime",
        $"sessionEndTime"
      ).cache()
  }

  /**
    * Finds median session duration.
    *
    * @return DF that contains median session duration (statistics)
    */
  def findMedianSessionDuration(eventsWithUserSessions: DataFrame): DataFrame = {
    eventsWithUserSessions
      .withColumn("duration", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))
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
      .withColumn(
        "duration",
        (unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))/60
      )

    withDuration
      .groupBy("category")
      .agg(
        countDistinct($"userId", when($"duration" < 1, $"userId")) as "lessThanOneMin",
        countDistinct($"userId", when($"duration" > 1 && $"duration" < 5, $"userId")) as "oneToFiveMins",
        countDistinct($"userId", when($"duration" > 5, $"userId")) as "moreThanFiveMins"
      )
  }

  /**
    * Prints DF with a limit - maximum 1 million rows.
    */
  def printDF(df: DataFrame): Unit = {
    df.show(1000000, truncate = 0)
  }

}