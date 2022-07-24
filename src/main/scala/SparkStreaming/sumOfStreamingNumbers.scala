package SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sumOfStreamingNumbers extends App{
  //Set Log Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the Spark Context
  val sc = new SparkContext("local[*]", "sumOfStreamingNumbers")

  // Creating the Spark Streaming Context
  val ssc = new StreamingContext(sc,Seconds(4))

  //store the previous state creating the checkpoint for statefull transformation
  ssc.checkpoint("src//resources//checkpoint//sumOfStreamingNumbers//")


  //Reading the lines from producer Dstream
  val lines = ssc.socketTextStream("localhost",9998)

  //reduceByWindow StateFullTransformation to sum results in particular window
  val sumCount = lines.reduceByWindow((x,y) => (x.toInt + y.toInt).toString(),Seconds(12),Seconds(4))

  sumCount.print()


  // Start the spark Streaming context
  ssc.start()

  //hold the termination until user terminate
  ssc.awaitTermination()

}
