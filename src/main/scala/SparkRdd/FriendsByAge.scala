package SparkRdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

//Find the Friends Connection count By Age
object FriendsByAge extends App{
  
  def parser(x: String) = {
    val words = x.split("::")
    val age = words(2).toInt
    val connection = words(3).toInt
    (age, connection)
  }

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the spark context
  val sc = new SparkContext("local[*]", "connectionCountByAge")

  // Reading the file
  val input = sc.textFile("src\\resources\\friendsdata.csv")

  // Parse the only required data
  val parsedData = input.map(parser)
  
  //(20,300)            //o/p (20,(200,1))
  
  //val mappedData = parsedData.map(x => (x._1,(x._2,1)))
   val mappedData = parsedData.mapValues(x => (x,1))

  //(20,(200,1))
  //(20,(300,1))                          o/p (20,(500,2))
  val aggregateData = mappedData.reduceByKey((x,y)=> (x._1 + y._1, x._2 + y._2))
  //(20,(500,2))                         (20,250)

  //val count = aggregateData.map(x => (x._1, (x._2._1 / x._2._2)))
  val count = aggregateData.mapValues(x => (x._1/x._2))
  
  
  val finalCount = count.sortBy(x => x._2,false)
  
  
  finalCount.collect().foreach(println)

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}