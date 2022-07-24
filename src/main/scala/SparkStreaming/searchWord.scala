package SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object searchWord extends App{

  //Set Log Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the Spark Context
  val sc = new SparkContext("local[*]", "searchWord")

  // Creating the Spark Streaming Context
  val ssc = new StreamingContext(sc,Seconds(4))

  //store the previous state creating the checkpoint for statefull transformation
  ssc.checkpoint("src//resources//checkpoint//searchWord//")


  //Reading the lines from producer Dstream
  val lines = ssc.socketTextStream("localhost",9998)


  val pairRdd = lines.flatMap(x => x.split(" ")).map(x => (x.toLowerCase(),1))

  val filterword = pairRdd.filter(x => x._1.startsWith("big"))

  def summaryFunc(x:Int, y:Int) = {
    x+y
  }

  def InverseFunc(x:Int, y:Int) = {
    x-y
  }

  //These "reduceByKeyAndWindow" with Named Function is a STATEFUL TRANSFORMATION & is working on FEW RDD's
  val words = filterword.reduceByKeyAndWindow(summaryFunc(_,_),InverseFunc(_,_),Seconds(12),Seconds(4))

  words.print()


  // Start the spark Streaming context
  ssc.start()

  //hold the termination until user terminate
  ssc.awaitTermination()

}
