package com.krasnyansky

import org.apache.spark.sql.SparkSession

package object spark {
  @transient lazy val spark: SparkSession = SparkSession.builder().appName("Spark Task").config("spark.master", "local").getOrCreate()
  spark.sparkContext.setLogLevel("OFF")
}
