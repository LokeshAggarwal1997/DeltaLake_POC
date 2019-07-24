import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit


import org.apache.log4j.{Level, Logger}


object lake extends App{

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val Spark = SparkSession.builder()
    .appName("Spark Delta example")
    .master("local")
    .config("spark.executor.instances", "2")
    .getOrCreate()

  import Spark.implicits._

//  //Reading a file
//  val df = Spark.read.option("header",true).csv("/home/knoldus/Desktop/products.csv")
//
//  //Creating a table
//  df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/tmp/delta-table/product")
//
// // Reading a table
//
//  val df1 = Spark.read.format("delta").load("/tmp/delta-table/product")
//
//
//  //Adding column to table
//
//  val newDF = df.withColumn("Country",lit("India"))
//
//  newDF.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("/tmp/delta-table/product")
//
//
//  //New Table
//  val df2 = Spark.read.format("delta").load("/tmp/delta-table/product")
//  df2.show()
/*--------------------------------------------------------------------------------------------*/


    val data = Spark.range(0, 5)
   data.write.format("delta").save("/tmp/delta-table")
  data.show()

  val data1 = Spark.range(5, 10)
  data1.write.format("delta").mode("overwrite").save("/tmp/delta-table")

  Spark.read.format("delta").load("/tmp/delta-table").show()

  val df = Spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
  df.show()

  val streamingDf = Spark.readStream.format("rate").load()
  val stream = streamingDf.select($"value" as "id").writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint").start("/tmp/delta-table")

  stream.awaitTermination(40000)
  /*--------------------------------------------------------------------------------------------*/
  //val stream2 = Spark.readStream.format("delta").load("/tmp/delta-table").writeStream.format("console").start()
 // val stream2 = Spark.readStream.format("delta").load("/tmp/delta-table").writeStream.format("console").start()




}

