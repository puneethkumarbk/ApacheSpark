package SparkStrucutedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.maven.shared.utils.logging.MessageUtils.level
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds,StreamingContext}

object statefullStreamingWordCount extends App{

  //Setting Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  //Creating the Spark Context
  val sc = new SparkContext("local[*]","statefulltransformationwordCount")

  //Creating the Spark Streaming Context
  val ssc = new StreamingContext(sc,Seconds(6))

  //store the previous state creating the checkpoint for statefull transformation
  ssc.checkpoint("src//resources//checkpoint//statefullStreamingWordCount//")

  //Reading the lines from producer Dstream
  val lines = ssc.socketTextStream("localhost",9994)

  //Transforming the Dstream
  val words = lines.flatMap(x => x.split(" "))

  val pairrdd = words.map(x => (x,1))


  def updatefunc(newValue:Seq[Int],previousState:Option[Int]):Option[Int] = {
    val newCount = previousState.getOrElse(0) + newValue.sum
    Some(newCount)
  }

  val wordCount = pairrdd.updateStateByKey(updatefunc)

  wordCount.print()

  ssc.start()

  ssc.awaitTermination()

}
