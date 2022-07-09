package SparkRdd

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

//Count the number of Word in Search data
object WordCount extends App {
  //setting the logging level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)


  //creating the spark context
  val sc = new SparkContext("local[*]", "wordCont Program")

  //loading the file to the rdd
  val base_rdd = sc.textFile("src\\resources\\search_data.txt")


  val words = base_rdd.flatMap(x => x.split(" ").map(x => x.toLowerCase()))

  val mappedRdd = words.map(x => (x,1))

  val reducedRdd = mappedRdd.reduceByKey((x,y) => x+y).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1))

  reducedRdd.collect().foreach(println)

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}