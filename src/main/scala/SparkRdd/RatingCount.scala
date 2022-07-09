package SparkRdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

// To find the Movie Rating Count
object RatingCount extends App{

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the spark context
  val sc = new SparkContext("local[*]", "ratingcount")

  // Reading the file
  val input = sc.textFile("src\\resources\\moviedata.data")
  
  
  val rating = input.map(x => x.split("\t")(2))
  
  //rating.countByValue()   -- It is combination of map and reducByKey
  
   val ratingmap = rating.map(x => (x,1))
  
  val ratingCount = ratingmap.reduceByKey((x,y) => x+y)
  
  ratingCount.collect.foreach(println)

/*
  val part1 = rating.repartition(10)
  println(part1.getNumPartitions)
  val part2 = rating.coalesce(3)
  println(part2.getNumPartitions)
  part2.take(3)
 */

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}