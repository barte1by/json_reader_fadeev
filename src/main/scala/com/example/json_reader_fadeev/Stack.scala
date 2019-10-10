package com.example.json_reader_fadeev

import org.apache.spark
import com.sun.xml.internal.ws.developer.Serialization
import org.json4s.jackson.parseJson
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
//import org.json4s.
//import scala.io.Source


object Stack extends App {
  //  implicit val formats = DefaultFormats // Brings in default date formats etc.
  implicit val formats = Serialization.formats(NoTypeHints)

  val spark: SparkSession = SparkSession
    .builder()
    .appName(name = "DZ")
    .master(master = "local[*]")
    .getOrCreate()

  val json = spark.sparkContext.textFile("/home/fadeev/Desktop/winemag-data-130k-v2.json")
  case class User(id: Int, country: String, points: Int, title: String, variety: String, winery: String)

  val decodeUser = parse(json).extract[User]

  //    for (line <- Source.fromFile("/home/fadeev/Desktop/winemag-data-130k-v2.json").getLines) {
//    val bookDetails = jsonStrToMap(line)
//      println(bookDetails)
    }

//    def jsonStrToMap(jsonStr: String): BookDetails = {
//      parse(jsonStr).camelizeKeys.extract[BookDetails]
//    }
//}
