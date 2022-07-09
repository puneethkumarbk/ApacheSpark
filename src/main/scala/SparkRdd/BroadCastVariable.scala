package SparkRdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source

//Broadcast variable to
object BroadCastVariable extends App {
  
  def boaring_words():Set[String] = {
    var boringWord:Set[String] = Set()
    
    val lines = Source.fromFile("src\\resources\\bingoWords.txt").getLines()
    
    for(line <- lines){
      boringWord+=line
    }
    
    boringWord
  }

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)


  // Creating the spark context
  val sc = new SparkContext("local[*]", "BrodcastVariable")
  
 val bingoSet = sc.broadcast(boaring_words)

  // Reading the file
 val baserdd = sc.textFile("src\\resources\\bigdatacampaigndata.csv")
 
 val parsed_data = baserdd.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))

  //Function chaining
 val data = parsed_data.flatMapValues(x => x.split(" ")).map(x => (x._2, x._1)).filter(x => !bingoSet.value(x._1)).reduceByKey(_+_).sortBy(x => x._2, false)
 
 data.collect.foreach(println)

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}