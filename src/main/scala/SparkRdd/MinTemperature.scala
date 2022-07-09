package SparkRdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.math.min


//Find the Minimum Temperature in the list
object MinTemperature extends App{

  //parse the only required columns
  def parser(x:String) = {
    val words = x.split(",")
    val id = words(0)
    val temp = words(3).toDouble
    
    (id,words(2),temp)
  }

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the spark context
  val sc = new SparkContext("local[*]", "yorN")

  // Reading the file
  val input = sc.textFile("src\\resources\\tempdata.csv")
  
 
 val parsedInput = input.map(parser)
 
 val minTemps = parsedInput.filter(x => x._2 == "TMIN").map(x => (x._1, x._3))
 
 val finalOut = minTemps.reduceByKey((x,y) => min(x,y))
 
 
 val result = finalOut.collect
 for (r <- result.sorted){
   val st = r._1
   val temp = r._2
   val formattedtemp = f"$temp%.2f F"
   
   println(s"$st minmum tempreature $formattedtemp")
 }


  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}