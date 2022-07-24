package SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object windowStreamingWordCount extends App {
  //Setting Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  //Creating the Spark Context
  val sc = new SparkContext("local[*]","windowTransformationWordCount")

  //Creating the Spark Streaming Context
  val ssc = new StreamingContext(sc,Seconds(2))

  //store the previous state creating the checkpoint for statefull transformation
  ssc.checkpoint("src//resources//checkpoint//WindowStreamingWordCount//")

  //Reading the lines from producer Dstream
  val lines = ssc.socketTextStream("localhost",9998)

  //Transforming the Dstream
  val words = lines.flatMap(x => x.split(" "))

  val pairrdd = words.map(x => (x,1))

  // It takes 4 parameter, Summary Function, Inverse Function, Window Size in Secs and Sliding Interval
  val wordCount = pairrdd.reduceByKeyAndWindow((x,y) => x+y, (x,y) => x-y, Seconds(20), Seconds(4))

  //Filter the words which value is zero (orelse it will still print (hello,0))
  val filteredWordCount = wordCount.filter(x => x._2 >0)

  //Action called
  filteredWordCount.print()

  // Start the spark Streaming context
  ssc.start()

  //hold the termination until user terminate
  ssc.awaitTermination()

}
