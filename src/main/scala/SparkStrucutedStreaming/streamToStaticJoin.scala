package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

// Stream to static is StateLess no need to maintain the state
object streamToStaticJoin extends App{

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

  // Schema for Streaming data
  val transactionSchema = StructType(List(
    StructField("card_id", LongType),
    StructField("amount", IntegerType),
    StructField("postal_code", LongType),
    StructField("post_id", LongType),
    StructField("transuction_dt", TimestampType)
  ))

  // Schema for static data
  val staticCardSchema = StructType(List(
    StructField("card_id", LongType),
    StructField("member_id", LongType),
    StructField("card_issue_date", TimestampType),
    StructField("country", StringType),
    StructField("state", StringType)
  ))



  // Read the streaming data from Socket
  val streamTransactionDf = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9998")
    .load()


  //processing
  val flattenStreamData = streamTransactionDf.select(from_json(col("value"), transactionSchema).alias("value"))
  val refinedTransactionDf = flattenStreamData.select("value.*")

  // Read the static data from File
  val staticDf = spark.read
    .format("csv")
    .schema(staticCardSchema)
    .option("path", "src\\resources\\card_data.csv")
    .load()

  // Joining Expression
  val joinExpr = refinedTransactionDf.col("card_id") === staticDf.col("card_id")

  // Joining Type
  val joinType = "leftouter"
  //Support joins for stream to  static df -- inner
  // LeftOuter if left table is Stream
  // Right Outer if Right table is stream

  val enrichedDf = refinedTransactionDf.join(staticDf, joinExpr, joinType).drop(staticDf.col("card_id"))

  // Write the output in the console
  val transactionQuery = enrichedDf.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "src//resources//checkpoint//streamToStaticJoin//")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .start()


  //hold the termination until user terminate
  transactionQuery.awaitTermination()
}
