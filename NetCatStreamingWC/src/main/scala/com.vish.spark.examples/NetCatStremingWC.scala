package com.vish.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NetCatStremingWC {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("NetCat Streaming WC")
      .config("spark.sql.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()

    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    linesDF.printSchema()

    val wordsDF = linesDF.select(expr("explode(split(value, ' ')) as word")) //using explode to convert from arrays of string to rows

    val countsDF = wordsDF.groupBy("word").count()

    val wordcountQuery = countsDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "chk-point-dir")
      .start()

    logger.info("Listening to localhost:9999")
    wordcountQuery.awaitTermination()

  }

}
