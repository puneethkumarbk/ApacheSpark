package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object structureStreamingWordCount extends App{
  //setting Logging Level
  Logger.getLogger("org").setLevel(Level.ERROR)


  //setting up the spark Conf
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","structureStreamingWordCount")
  sparkConf.set("spark.master","local[2]")

  //creating sparkSession
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  //setting the number of partitions because after shuffle it reaches to 200 partitions
  spark.conf.set("spark.sql.shuffle.partitions",3)
  //Shutdown Gracefully During Shutdown
  spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

  val linesDf = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9998")
    .load()

  val words = linesDf.selectExpr("explode(split(value, ' ')) as word")
  val wordsCount = words.groupBy("word").count()


  val writeQuery = wordsCount.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "src//resources//checkpoint//structureStreamingWordCount//")
    //.trigger(Trigger.ProcessingTime("30 seconds"))  //time setting for to trigger
    .start()

  //hold the termination until user terminate
  writeQuery.awaitTermination()
}
