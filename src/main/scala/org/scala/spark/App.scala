package org.scala.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
 * @author Chandra
 */
object App {
  def readFiles(): Unit ={
    val spark = SparkSession.builder().appName("Log Analyser").
      config("spark.master","local").getOrCreate()

    //read source
    import spark.implicits._
    val fileDF =spark.read.
      text("src/main/resources/data/FlumeData.1600343147942")
      //option("path","hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/logAnalyser/FlumeData.1600343147942").
    //fileDF.printSchema()

    //Fetch required data
    val ipDF = fileDF.select(regexp_extract($"value","""^.*\s/([^\s]+)$""", 1).as("ip"))
    ipDF.show(3)

    ipDF.printSchema()
   // val cleanDF = ipDF.select(regexp_extract($"ipAddr","""^.*\s/([^\s]+)$""", 1).as("ip"))

    //aggregated DF
    val aggDF = ipDF.groupBy(col("ip")).count().as("count").
      orderBy(col("count").desc)
  }

  def main(args : Array[String]) {
    println( "Program about to start!" )
    readFiles()
  }
}