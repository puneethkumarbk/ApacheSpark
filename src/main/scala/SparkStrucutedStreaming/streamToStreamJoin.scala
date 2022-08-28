package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

// Stream to Stream is StateFull we need to maintain the state..
object streamToStreamJoin extends App{

  //setting Logging Level
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create the spark Session
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("streamToStaticJoin")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  // Schema for Streaming data Impression
  val impressionSchema = StructType(List(
    StructField("impressionID", StringType),
    StructField("ImpressionTime", TimestampType),
    StructField("CampaignName", StringType)
  ))

  // Schema for Streaming data Click
  val clickSchema = StructType(List(
    StructField("clickID", StringType),
    StructField("ClickTime", TimestampType),
  ))



  // Read the streaming Impression data from Socket 1
  val streamImpressionDf = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9995")
    .load()


  //processing impression stream data
  val flattenImpressionData = streamImpressionDf.select(from_json(col("value"), impressionSchema).alias("value"))
  val refinedImpressionDf = flattenImpressionData.select("value.*").withWatermark("ImpressionTime", "30 minute")


  // Read the streaming click  data from Socket 2
  val streamClickDf = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9996")
    .load()


  //processing click stream data
  val flattenStreamClickData = streamClickDf.select(from_json(col("value"), clickSchema).alias("value"))
  val refinedClickDf = flattenStreamClickData.select("value.*").withWatermark("ClickTime", "30 minute")


  // Joining Expression
  val joinExpr = expr("impressionID == clickID AND ClickTime BETWEEN ImpressionTime AND ImpressionTime + interval 15 minute")

  // Joining Type
  val joinType = "leftOuter"
  //Support joins for stream to  stream df -- inner
  // LeftOuter possible if Right  table is watermarked and maximum time constraint between two tables.
  // Right Outer possible if Left  table is watermarked and maximum time constraint between two tables.

  val enrichedDf = refinedImpressionDf.join(refinedClickDf, joinExpr, joinType).drop(refinedClickDf.col("clickID"))

  // Write the output in the console
  val transactionQuery = enrichedDf.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "src//resources//checkpoint//streamToStreamJoin//")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .start()


  //hold the termination until user terminate
  transactionQuery.awaitTermination()

}
