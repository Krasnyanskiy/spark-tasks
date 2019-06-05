package com.krasnyansky.spark

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

    // ============================================ Task #1 ============================================

    val groupedEvents = events.groupBy(window($"eventTime", "5 minutes"), $"eventType")

    val sessionStatistic = groupedEvents.agg(
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
      ).cache() // cache?

    sessionStatistic.show(1000000, truncate = 0)

    // ============================================ Task #2 ============================================

    // Get Median (2.1)

    val medianSessionDuration = sessionStatistic
      .withColumn("duration", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))
      .groupBy("category")
      .agg(callUDF("percentile_approx", col("duration"), lit(0.5)) as "median")

    medianSessionDuration.show(10000000, truncate = 0)

    // Find number of unique users (2.2)

    val withDuration = sessionStatistic
      .withColumn(
        "duration",
        (unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))/60
      )

    val uniqueUsers = withDuration
      .groupBy("category")
      .agg(
        countDistinct($"userId", when($"duration" < 1, $"userId")) as "lessThanOneMin",
        countDistinct($"userId", when($"duration" > 1 && $"duration" < 5, $"userId")) as "oneToFiveMins",
        countDistinct($"userId", when($"duration" > 5, $"userId")) as "moreThanFiveMins"
      )

    uniqueUsers.show(10000000, truncate = 0)

    // Find find top 10 products (2.3)

    val rankedProducts = ???

  }
}