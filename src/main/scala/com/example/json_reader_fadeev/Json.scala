package com.example.json_reader_fadeev

import org.json4s._
import scala.io.Source
import org.json4s.jackson.JsonMethods._

object Json extends App {

  val filename = "/home/fadeev/Desktop/winemag-data-130k-v2.json"

  implicit val formats = DefaultFormats

  case class Users(
                    id: Option[Int],
                    country: Option[String],
                    points: Option[Int],
                    price: Option[Int],
                    title: Option[String],
                    variety: Option[String],
                    winery: Option[String])

    for (line <- Source.fromFile(filename).getLines) {
    val Users = jsonStrToMap(line)
     println(Users)
    }

    def jsonStrToMap(jsonStr: String): Users = {
     parse(jsonStr).camelizeKeys.extract[Users]
    }
}
