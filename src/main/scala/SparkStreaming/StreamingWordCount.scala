package SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount extends App{

  //Setting Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  //Creating the Spark Context
  val sc = new SparkContext("local[*]", "StreamingWordCount")

  //Creating the Spark Streaming Context
  val ssc = new StreamingContext(sc, Seconds(5))

  //Reading the lines from producer Dstream
  val lines = ssc.socketTextStream("localhost", 9998)

  //Transforming the Dstream
  val words = lines.flatMap(x => x.split(" "))

  val pairs = words.map(x => (x,1))

  val wordCount = pairs.reduceByKey((x,y) => x+y)

  wordCount.print()

  //To Start the Spark streaming Context
  ssc.start()

  //hold the termination until user terminate
  ssc.awaitTermination()

}
