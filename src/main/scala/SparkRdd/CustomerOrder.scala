package SparkRdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

//Filter the Customer Order
object CustomerOrder extends App{

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the spark context
  val sc = new SparkContext("local[*]", "customerOrderdetails")

  // Reading the file
  val baseRdd = sc.textFile("src\\resources\\customerorders.csv")

  val mappedRdd = baseRdd.map(x => {
   val nums = x.split(",")
   (nums(0), nums(2).toDouble.toInt)
 })
 val gropedRes = mappedRdd.reduceByKey(_+_)
 val filteredData = gropedRes.filter(z => z._2 > 500)
  
 //println(filteredData.count())
 filteredData.saveAsTextFile("src\\resources\\output\\CustomerOrder")

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
 
}