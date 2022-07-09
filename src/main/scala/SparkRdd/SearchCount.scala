package SparkRdd

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

// Find the Frequency of each word in Descending order
object SearchCount extends App {

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the spark context
  val sc = new SparkContext("local[*]","frequencyCount")

  // Reading the file
  val input = sc.textFile("src\\resources\\search_data.txt")

  val words = input.flatMap(x => x.split(" "))
  //(data, big, BIG, course....)
  
  val wordslc = words.map(x => x.toLowerCase())
  //(data, big, big, course....)
  
  val wordsMap = wordslc.map(x => (x,1))
  //(data,1), (big,1), (big,1)... course....)
  
  
  
  val finalCount = wordsMap.reduceByKey((x,y)=> x+y)
  //(data,1) (big,2)
  
  
  //val swapWord = finalCount.sortBy(x => x._2,false)

  val swapWord = finalCount.map(x => (x._2, x._1)).sortByKey(false).map(x =>(x._2,x._1))
  //swap -- sort -- swap to original case  (big,2) (data, 1)


  swapWord.collect().foreach(println)

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}