package com.example.json_reader_fadeev

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.FullTypeHints
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization


object JsonReaderFadeev extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName(name = "JsonReader")
    .master(master = "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val InputFilename: String = "/home/fadeev/Desktop/winemag-data-130k-v2.json"  //Linux

  case class JSON(
                   id: Option[Int],
                   country: Option[String],
                   points: Option[Int],
                   price: Option[Int],
                   title: Option[String],
                   variety: Option[String],
                   winery: Option[String]
                 )

  implicit val formats = {
    Serialization.formats(FullTypeHints(List(classOf[JSON])))
  }

  val JsonRdd: RDD[String] = sc.textFile(InputFilename)

  JsonRdd.map(
    file => parse(file)
      .extract[JSON])
    .foreach(file => println(file)
    )
}