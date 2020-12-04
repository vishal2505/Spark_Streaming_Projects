package com.vish.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object DataFileStreaming extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Data File Streaming")
      .config("spark.sql.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    val rawDF = spark.readStream
      .format("json")
      .option("path", "input")
      .option("maxFilesPerTrigger", 1)
      .load()

    val explodeDF = rawDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
      "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
      "DeliveryAddress.State", "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")

    val invoiceWriterQuery = flattenedDF.writeStream
      .format("json")
      .option("path", "output")
      .option("checkpointLocation", "chk-point-dir")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .queryName("Flattened Invoice Writer")
      .start()

    logger.info("Flattened Invoice Writer started...")
    invoiceWriterQuery.awaitTermination()


  }

}
