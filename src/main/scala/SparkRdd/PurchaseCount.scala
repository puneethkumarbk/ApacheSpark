package SparkRdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

//To Find the Customer purchase count
object PurchaseCount extends App{

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the spark context
  val sc = new SparkContext("local[*]", "purchasecount")

  // Reading the file
  val input = sc.textFile("src\\resources\\customerorders.csv")
 
  
  val listInfo = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat))
  
  val finalCount = listInfo.reduceByKey((x,y) => x+y)
  
  val sortedCount = finalCount.sortBy(x => x._2 , false)
  
  val result = sortedCount.collect()


  result.foreach(println)

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}