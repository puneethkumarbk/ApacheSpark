package SparkRdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RepartitionandCoalesce extends App{

  // Set Logger Level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Creating the spark context
  val sc = new SparkContext("local[*]", "finding top movies")

  // Reading the file
  val baseRdd = sc.textFile("src\\resources\\ratings.dat")

  val partitionRdd = baseRdd.repartition(20)

  println(partitionRdd.getNumPartitions)

  val rdd3 = partitionRdd.coalesce(10)
  println(rdd3.getNumPartitions)


  println(sc.defaultMinPartitions)


  //To hold the spark context to check the DAG
  scala.io.StdIn.readLine()

  //Stop the Spark Context
  sc.stop()
}
