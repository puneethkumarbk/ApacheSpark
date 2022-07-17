package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object streamingLineCount extends App{
  //Set Log Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the Spark Context
  val sc = new SparkContext("local[*]", "streamingLineCount")

  // Creating the Spark Streaming Context
  val ssc = new StreamingContext(sc,Seconds(4))

  //store the previous state creating the checkpoint for statefull transformation
  ssc.checkpoint("src//resources//checkpoint//streamingLineCount//")


  //Reading the lines from producer Dstream
  val lines = ssc.socketTextStream("localhost",9998)



  //countByWindow -- It count the number of value in by window   Ex : If one typed three times result = (1,3)
  val sumCount = lines.countByValueAndWindow(Seconds(12),Seconds(4))

  //countByWindow -- It count the number of Lines in by window    Ex : If one typed three times result = 3
  val sumCount2 = lines.countByWindow(Seconds(12),Seconds(4))

  sumCount.print()
  sumCount2.print()


  // Start the spark Streaming context
  ssc.start()

  //hold the termination until user terminate
  ssc.awaitTermination()

}
