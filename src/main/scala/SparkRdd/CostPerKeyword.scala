package SparkRdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source

//Calculate the cost of each keyword and exclude some of the bingo-words(unrelated words)
//Broadcast variable used to broadcast the info to send each executor and compare with distributed big files to avoid shuffling
object CostPerKeyword extends App{
  
  def bingoWords():Set[String]= {

    var brodCastWords:Set[String]= Set()

    //Reading the Broadcast File
    val lines = Source.fromFile("src\\resources\\bingoWords.txt").getLines()
    
    for (line <- lines) {
      brodCastWords += line
    }
    
   brodCastWords
  }

  //Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the Spark context
  val sc = new SparkContext("local[*]", "costPerKey")

  //Creating Broadcast variable
  val nameSet = sc.broadcast(bingoWords)

  // Reading the file
  val inputRdd = sc.textFile("src\\resources\\bigdatacampaigndata.csv")

  //big data training 24.45       //24.45 bigdata training
  val mapData = inputRdd.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))
   
      
  //24.45 bigdata training   
  //(24.45,big)(24.45, data)
  
  
  val words = mapData.flatMapValues(x => x.split(" "))
  
  //(24.45,big)(24.45, data)       //(big,24.45)(data,24.45)
  val dataCount = words.map(x => (x._2.toLowerCase(), x._1))
  
  //filter the boring words
  //(big,24.45)(data,24.45)  still remain
  // (is,24.45)--boring word ignore
  val filterData  = dataCount.filter(x => !(nameSet.value(x._1)))

  val finalRes = filterData.reduceByKey((x,y)  => (x+y)).sortBy(x =>x._2,false)

 finalRes.collect.foreach(println)

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}