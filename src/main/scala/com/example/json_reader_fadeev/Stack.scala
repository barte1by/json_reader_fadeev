package com.example.json_reader_fadeev

import org.json4s._
import scala.io.Source
import org.json4s.jackson.JsonMethods._

object Stack extends App {
  implicit val formats = DefaultFormats // Brings in default date formats etc.

  case class Users(
                    id: Option[Int],
                    country: Option[String],
                    points: Option[Int],
                    price: Option[Int],
                    title: Option[String],
                    variety: Option[String],
                    winery: Option[String])

    for (line <- Source.fromFile("/home/fadeev/Desktop/winemag-data-130k-v2.json").getLines) {
    val Users = jsonStrToMap(line)
     println(Users)
    }

    def jsonStrToMap(jsonStr: String): Users = {
     parse(jsonStr).camelizeKeys.extract[Users]
    }
}
