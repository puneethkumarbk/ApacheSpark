package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object waterMarkExample extends App{
  //setting Logging Level
  Logger.getLogger("org").setLevel(Level.ERROR)


  // Create the spark Session
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("waterMarkExample")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  // Schema
  val ordersSchema = StructType(List(
    StructField("card_id", LongType),
    StructField("amount", IntegerType),
    StructField("postal_code", LongType),
    StructField("post_id", LongType),
    StructField("transuction_dt", TimestampType)
  ))

  // Read the data from Socket
  val ordersDf = spark.readStream
    .format("socket")
    .option("host" , "localhost")
    .option("port", "9998")
    .load()

  //Process
  //ordersDf.printSchema() -- value: string (nullable = true)
  val valueDf = ordersDf.select(from_json(col("value"), ordersSchema).alias("value"))

  //Selecting the fields inside struct fields to avoid the nesting things
  val refinedOrders = valueDf.select("value.*")

  val windowAggDf = refinedOrders
    //with water mark takes event time  and duration
    .withWatermark("transuction_dt", "30 minute")
    //.groupBy(window(col("transaction_dt"), "15 minute"))
    .groupBy(window(col("transuction_dt"), "15 minute", "5 minute"))   //sliding window it overlaps
    .agg(sum("amount").alias("Total_invoice"))



  val outputDf = windowAggDf.select("window.start", "window.end", "Total_invoice")

  // Write the output in the console
  val ordersQuery = outputDf.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "src//resources//checkpoint//waterMarkExample//")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  //hold the termination until user terminate
  ordersQuery.awaitTermination()
}
