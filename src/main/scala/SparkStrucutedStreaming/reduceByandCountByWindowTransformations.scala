package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object reduceByandCountByWindowTransformations extends App{

  //Set Log Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the Spark Context
  val sc = new SparkContext("local[*]", "reduceByAndCountByWindow")

  // Creating the Spark Streaming Context
  val ssc = new StreamingContext(sc,Seconds(4))

  //store the previous state creating the checkpoint for statefull transformation
  ssc.checkpoint("src//resources//checkpoint//reduceByandCountByWindow//")

  //Reading the lines from producer Dstream
  val lines = ssc.socketTextStream("localhost",9998)


  def summaryFunc(x:String,y:String):String = {
    (x.toInt + y.toInt).toString
  }

  def InverseFunc(x:String, y:String):String = {
    (x.toInt - y.toInt).toString
  }

  //Reduce by window is take 4 parameters summryFunc,InverseFunc, window Size and sliding interval
  // reduceByWindow expcect single rdd(not pair) and combine all the values ex. 4 5 6 7   it will summup all the value and return res
  val sumCount = lines.reduceByWindow(summaryFunc, InverseFunc, Seconds(12), Seconds(4))

  /*
  val sumCount = lines.countByWindow(Seconds(12), Seconds(4))
*/

  //Action
  sumCount.print()

  // Start the spark Streaming context
  ssc.start()

  //hold the termination until user terminate
  ssc.awaitTermination()
}
