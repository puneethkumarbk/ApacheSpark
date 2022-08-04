package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object fileDataSource extends App{
  //setting Logging Level
  Logger.getLogger("org").setLevel(Level.ERROR)


  // Create the spark Session
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("fileDataSource")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  // Read the file from Source
  val ordersDF = spark
    .readStream
    .format("json")
    .option("path", "src//resources//streamingdatasourcefile//inputFiles//")
    .option("maxFilesPerTrigger",1)
    .load()


  // Process
  ordersDF.createOrReplaceTempView("orders")

  val completed_orders = spark.sql("select * from orders where order_status = 'COMPLETE'")

  // Write to the sink
  val ordersQuery = completed_orders
    .writeStream
    .format("json")
    .outputMode("append")
    .option("path","src//resources//streamingdatasourcefile//outputFiles//")
    .option("checkpointLocation", "src//resources//checkpoint//fileDataSource//")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .option("cleanSource","archive")
    .option("sourceArchiveDir", "src//resources//streamingdatasourcefile//outputFiles2//")
    .start()

  ordersQuery.awaitTermination()

}
