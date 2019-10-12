package com.example.json_reader_fadeev

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

object Json extends App {

  val filename = "D:\\Dropbox\\obrozavanie\\OTUS\\5\\winemag-data-130k-v2.json" //Windows
  //  val filename = "/home/fadeev/Desktop/winemag-data-133k-v2.json"  //Linux

  implicit val formats = DefaultFormats

  case class Json(
                    id: Option[Int],
                    country: Option[String],
                    points: Option[Int],
                    price: Option[Int],
                    title: Option[String],
                    variety: Option[String],
                    winery: Option[String])

    for (line <- Source.fromFile(filename).getLines) {
    val Jsonline = jsonStrToMap(line)
     println(Jsonline)
    }

    def jsonStrToMap(jsonStr: String): Json = {
     //parse(jsonStr).camelizeKeys.extract[Users]
     parse(jsonStr).extract[Json]
    }
}
