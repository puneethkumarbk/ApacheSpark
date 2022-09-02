package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, from_json, unix_timestamp, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType, TimestampType}

object WeatherHistoryTempGrouping extends App{

  //setting Logging Level
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create the spark Session
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("GroupingTemperature")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .getOrCreate()



  //schema
  val weatherSchema = StructType(List(StructField("DateTime", TimestampType),
    StructField("Temperature", DoubleType),
    StructField("Humidity", FloatType),
    StructField("WindSpeed", DoubleType),
    StructField("Pressure", DoubleType),
    StructField("Summary", StringType)))


  // Read the data from Socket
  val weatherDf = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9998")
    .load()

  //{"DateTime":"2014-01-08 03:31:52","Temperature":5.438888889,"Humidity":0.88,"WindSpeed":3.7191,"Pressure":1012.23,"Summary":"Partly Cloudy"}
  //{"DateTime":"2014-01-08 03:31:50","Temperature":5.38888889,"Humidity":0.88,"WindSpeed":3.7191,"Pressure":1012.23,"Summary":"Partly Cloudy"}



  //process
  val weatherStructDf = weatherDf.select(from_json(col("value"), weatherSchema).alias("value"))
  val weatherData = weatherStructDf.select("value.*").withWatermark("DateTime", "30 minute")

  weatherData.printSchema()

  val weatherGroupedData = weatherData
    .groupBy(window(col("DateTime"), "15 minute","5 minute"))
    .agg(avg(col("Temperature")).alias("AVGTemperature"))

  val weatherSelectedData = weatherGroupedData.select("window.start","window.end", "AVGTemperature")

  // Write the output in the console
  val processedQuery = weatherSelectedData.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "src//resources//checkpoint//WeatherHistoryTempGrouping_checkpoint")
    //.option("path", "src//resources//output//WeatherHistoryTempGrouping_output")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  //hold the termination until user terminate
  processedQuery.awaitTermination()

}
