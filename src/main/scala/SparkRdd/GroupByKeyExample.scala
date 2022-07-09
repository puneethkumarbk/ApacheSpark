package SparkRdd

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

//Demonstrate the Groupbykey Example
object GroupByKeyExample extends App{

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the spark context
  val sc = new SparkContext("local[*]","groupbyexample")

  // Reading the file
  val baserdd = sc.textFile("src\\resources\\bigLog.txt")
  
  val mappedRdd = baserdd.map(x => {
    var fields = x.split(",")
    (fields(0), fields(1))
  })
  
  //.take(10).foreach(println)
  
  mappedRdd.groupByKey().collect().foreach(x => println(x._1, x._2.size))
  
  //groupRes.take(10)

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}

