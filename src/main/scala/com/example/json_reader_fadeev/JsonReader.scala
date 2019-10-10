package com.example.json_reader_fadeev

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

object JsonReader extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName(name = "DZ")
    .master(master = "local[*]")
    .getOrCreate()

  val FILE = spark.sparkContext.textFile("/home/fadeev/Desktop/winemag-data-130k-v2.json")
  val sc = spark.sparkContext

  implicit val formats = DefaultFormats

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

//  val decodedUser = parse(FILE).extract[Users]

  val firstRDD: RDD[String] = sc.parallelize(List(Users: Users) )
  println(firstRDD)


  /*  val json = spark.read.json("/home/fadeev/Desktop/winemag-data-130k-v2.json")
  json.show()
*/

/*  val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF", " " -> "#FFFFFF")
  println( "Keys in colors : " + colors.keys )
  println( "Values in colors : " + colors.values )
  println( "Check if colors is empty : " + colors.isEmpty )
*/
  val t = (4,3,2,1)
  val sum = (t._1 + t._1 ) * t._3 + t._1
  println(sum)

  // Primer
/*  val data = Array(1, 2, 3, 4, 5)
  val firstRDD: RDD[Int] = sc.parallelize(data)
  val result = firstRDD
    .map( x => x+1)
    .flatMap(x => List (x, x*2, x*3))
    .filter(x => x >3 )
    .collect()
    println {
      result.toList
   }
 */
}


