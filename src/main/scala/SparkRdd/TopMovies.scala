package SparkRdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

//Find the Top Movies which has more than 4.5 rating and watched more than 500 people
object TopMovies extends App{

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the spark context
  val sc = new SparkContext("local[*]", "finding top movies")
  
  //rating List file
  val baseRdd = sc.textFile("src\\resources\\ratings.dat")
  
  //movie List File
  val movieList = sc.textFile("src\\resources\\movies.dat")
  
  val mappedRdd = baseRdd.map(x => {
    val fields = x.split("::")
    
    (fields(1), fields(2))
  })
  
  //(1193,5)
  //(1193,4)
  
  
  //op (1193,(5,1))
  val avgRdd = mappedRdd.map(x => (x._1, (x._2.toDouble, 1.0)))
  
      //ip (1193,(5,1))
      //ip (1193,(5,1))
  
       //op (1193,(5,2))
 val ratingcount = avgRdd.reduceByKey((x,y) => (x._1+y._1 , x._2+y._2))
 //ip (1193,(10,2))

  //Considering only movie which has been viewed from more than 500 people
 val filteruser = ratingcount.filter(x => x._2._2 > 500)
 
 
 val finalrating = filteruser.mapValues(x => (x._1/x._2))
 //op (1193,2.5)
 
 
 //ip (1193,2.5)
 //Filtering the only movies rated greater than 4.5
 val filterrating = finalrating.filter(x => x._2 > 4.5)
 
  val moviesdata = movieList.map(x => {
    val fields = x.split("::")
    
    (fields(0), fields(1))
  })
 //(1193,moviename)
  
  
  //(id , (rating, movie))
  //After considering rating field joining with movie data to find the movie names
 val joindata = filterrating.join(moviesdata)


  val moviename = joindata.map(x => x._2._2)

  moviename.collect.foreach(println)

  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}