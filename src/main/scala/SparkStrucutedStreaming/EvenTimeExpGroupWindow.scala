package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}


object EvenTimeExpGroupWindow extends App{
  //setting Logging Level
  Logger.getLogger("org").setLevel(Level.ERROR)


  // Create the spark Session
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("EvenTimeExpGroupWindow")
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
    StructField("transuction_dt", StringType)
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
  //valueDf.printSchema()

  val refinedOrders = valueDf.select("value.*")
  /* //refinedOrders.printSchema()
  root
  |-- card_id: integer (nullable = true)
  |-- amount: integer (nullable = true)
  |-- postal_code: integer (nullable = true)
  |-- post_id: integer (nullable = true)
  |-- transuction_dt: string (nullable = true) */

  val windowAggDf = refinedOrders.groupBy(window(col("transuction_dt"), "15 minute"))
    .agg(sum("amount").alias("Total_invoice"))

  //windowAggDf.printSchema()
  /* root
 |-- window: struct (nullable = false)
 |    |-- start: timestamp (nullable = true)
 |    |-- end: timestamp (nullable = true)
 |-- Total_invoice: long (nullable = true)*/


  val outputDf = windowAggDf.select("window.start", "window.end", "Total_invoice")

  // Write the output in the console
  val ordersQuery = outputDf.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "src//resources//checkpoint//EvenTimeExpGroupWindow//")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  //hold the termination until user terminate
  ordersQuery.awaitTermination()

}
