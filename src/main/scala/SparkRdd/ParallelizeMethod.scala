package SparkRdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

//To Load the data from local list and create the rdd out of it
object ParallelizeMethod extends App {

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)


  // Creating the spark context
  val sc = new SparkContext("local[*]", "LogLevel count")
  
  val myList = List("WARN: Tuesday 2021 01 10 first", "ERROR : Thursday 2021 01 12 second", "ERROR : friday 2021 01 593 third")

  // Creating the Rdd from list using parallelize method
  val LogLevelRdd = sc.parallelize(myList)
  
  val pairRdd = LogLevelRdd.map(x=> { 
    val words = x.split(":")
    val logWord = words(0)
    (logWord, 1)
  })
  
  //"WARN: Tuesday 2021 01 10 first", "ERROR : Thursday 2021 01 12 second", "ERROR : friday 2021 01 593 third"
  
  //(WARN,1) (ERROR,1) (ERROR,1)
  
  
  val logCount = pairRdd.reduceByKey((x,y) => x+y)
  
  logCount.collect().foreach(println)

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}