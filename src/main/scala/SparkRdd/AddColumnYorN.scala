package SparkRdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

//Add the column based on the conditions

object AddColumnYorN extends App{
  // create new column and add Y if age is greater than 18 else add N
  def ageCheck(x:String) = {

    val words = x.split(",")
    
    val age = words(1).toInt
    
    if (age > 18)
      (words(0), words(1),words(2), "Y")
    else (words(0), words(1),words(2), "N")  
  }

  //Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the spark context
  val sc = new SparkContext("local[*]", "yorN")

  // Reading the file
  val input = sc.textFile("src\\resources\\data.txt")
  

  //check the each line element and add Y/N
 val parsedInput = input.map(ageCheck)
 
 parsedInput.collect.foreach(println)

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}