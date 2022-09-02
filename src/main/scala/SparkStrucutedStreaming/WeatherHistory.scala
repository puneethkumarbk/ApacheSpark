package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType, TimestampType}

object WeatherHistory extends App{

  //setting Logging Level
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create the spark Session
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("WeatherHistory")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()


  //schema
  val weatherSchema = StructType(List(StructField("DateTime", StringType),
    StructField("Temperature", DoubleType),
    StructField("Humidity", FloatType),
    StructField("WindSpeed", DoubleType),
    StructField("Pressure", DoubleType),
    StructField("Summary", StringType)))

  // Read the data from file source
  val weatherDf = spark.readStream
    .format("csv")
    .schema(weatherSchema)
    //.option("header", true)
    //.option("inferSchema", true)
    .option("path", "src//resources//weatherHistory//")
    .option("maxFilesPerTrigger", 1)
    .load()

  //val weatherDf2 = weatherDf.withColumn("DateTime", unix_timestamp(col("DateTime")).cast(TimestampType))
  //weatherDf2.printSchema()

  //Process
  weatherDf.createOrReplaceTempView("processedWeatherDf")

  val processedWeatherDf = spark.sql("SELECT * FROM processedWeatherDf WHERE WindSpeed < 11.0 AND Summary == 'Partly Cloudy'")


  //val processedWeatherDf = weatherDf.select("*").where(abs(col("WindSpeed" )) < 11.00 && col("Summary") == "Partly Cloudy")

  // Write the output in the console
  val processedQuery = weatherDf.writeStream
    .format("json")
    .outputMode("append")
    .option("checkpointLocation", "src//resources//checkpoint//WeatherHistory_checkpoint")
    .option("path", "src//resources//output//WeatherHistory_output")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .start()

  //hold the termination until user terminate
  processedQuery.awaitTermination()
}
