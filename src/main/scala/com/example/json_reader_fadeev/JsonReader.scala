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

  val filename = "/home/fadeev/Desktop/winemag-data-130k-v2.json"
  case class Users(
                    id: Option[String],
                    country: Option[String],
                    points: Option[String],
                    price: Option[String],
                    title: Option[String],
                    variety: Option[String],
                    winery: Option[String])

  val lines = sc.textFile(filename)

  println(lines)

    for (line <- Source.fromFile(filename).getLines) {
      val datas = jsonStrToMap(line)
      //println(datas)
    }

  def jsonStrToMap(jsonStr: String): Users = {
    parse(jsonStr).camelizeKeys.extract[Users]
  }

  def readFile(filename: String): Seq[String] = {
    val bufferedSource = scala.io.Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toSeq
    bufferedSource.close
    lines
  }


//  val decodedUser = parse(datata)

 val RDD = sc.parallelize(lines : Seq[])
 println(RDD)


  for ( a <- 1 to 10) {   println( "Value of a: " + a )}

  /*  val json = spark.read.json("/home/fadeev/Desktop/winemag-data-130k-v2.json")
  json.show()
*/

/*  val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF", " " -> "#FFFFFF")
  println( "Keys in colors : " + colors.keys )
  println( "Values in colors : " + colors.values )
  println( "Check if colors is empty : " + colors.isEmpty )

  val t = (4,3,2,1)
  val sum = (t._1 + t._1 ) * t._3 + t._1
  println(sum)

  // Primer
  val data = Array(1, 2, 3, 4, 5)
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


