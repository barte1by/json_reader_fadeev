package com.example.json_reader_fadeev

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object JsonReader extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName(name = "DZ")
    .master(master = "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
//  val filename = "/home/fadeev/Desktop/winemag-data-133k-v2.json"  //Linux
  val filename = "D:\\Dropbox\\obrozavanie\\OTUS\\5\\winemag-data-130k-v2.json"  //Windows
  implicit val formats = DefaultFormats

  case class JSON(
                    id: Option[Int],
                    country: Option[String],
                    points: Option[Int],
                    price: Option[Int],
                    title: Option[String],
                    variety: Option[String],
                    winery: Option[String])

  val lines = sc.textFile(filename) // use file
  val myRecRdd: RDD[String] = lines
  val a = myRecRdd.collect()
  val b = sc.parallelize(a, 4).collect()

  for (line <- b) {
    val Users = jsonStrToMap(line)
    println(Users)
  }

  def jsonStrToMap(jsonStr: String): JSON = {
  parse(jsonStr).camelizeKeys.extract[JSON]
  }
}


