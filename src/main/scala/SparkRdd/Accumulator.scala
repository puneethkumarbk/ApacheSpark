package SparkRdd

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

//Accumulator variable using to count the empty lines
object Accumulator extends App{

 //Set Logger level to Error
  Logger.getLogger("org").setLevel(Level.ERROR)

 // Creating the spark context
 val sc = new SparkContext("local[*]", "costPerKey")

 //reading the text file
  val inputRdd = sc.textFile("src\\resources\\sampleFile.txt")
  
  val acc = sc.longAccumulator("my accumulator")
  
  
 inputRdd.foreach(x => if (x == "" )acc.add(1))
 
 
 println(acc.value)

 //To hold the spark context to check the DAG
 scala.io.StdIn.readLine()

 //Stop the Spark Context
 sc.stop()
}