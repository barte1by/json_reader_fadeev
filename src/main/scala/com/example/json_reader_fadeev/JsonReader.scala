package com.example.json_reader_fadeev

import com.example.json_reader_fadeev.Json.{filename, jsonStrToMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import collection.immutable.IndexedSeq
import scala.io.Source

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
  // lines.foreach(println) // print file

//  import spark.implicits._
//  val jsonline = spark.read.json(filename).as[JSON]
//  lines.foreach(println),
//  val ee : RDD[String] = lines.collect().toList
//def mmain(args: Array[String]): Unit = {
  val myRecRdd: RDD[String] = lines
  val a = myRecRdd.collect()
  val b = sc.parallelize(a, 4).collect()

  for (line <- b) {
    val Users = jsonStrToMap(line)
    println(Users)
  }

//  println(b.mkString)



//  val s = Seq(lines)
//  val ebRDD : RDD[JSON] = sc.parallelize(lines : RDD[JSON])
  //val ebRDD : RDD[Any] = sc.parallelize(Array <- lines )
  //ebRDD.toDebugString

 // for ( line <- lines.foreach(println) {
  //}


  def jsonStrToMap(jsonStr: String): JSON = {
  parse(jsonStr).camelizeKeys.extract[JSON]
  }

//  for ( a <- 1 to 10) {   println( "Value of a: " + a )}

  /*
  val sampleArray = Array(
    ("FRUIT", List("Apple", "Banana", "Mango")),
    ("VEGETABLE", List("Potato", "Tomato")))

  val sampleRdd = sc.parallelize(sampleArray)
  sampleRdd.foreach(println)


  val json = "categories" -> sampleRdd.collect().toList.map{
    case (name, nodes) =>
      ("name", name) ~
        ("nodes", nodes.map{
          name => ("name", name)
        })
  }

  println(compact(render(json)))
*/

  //val lineLengths = lines.map(sff => sff.length)
  //val totalLength = lineLengths  .reduce((a, b)=> a + b)
 // println(totalLength)







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


*/
/*  // Primer
  val data = Array(1, 2, 3, 4, 5, "ds")
  val firstRDD : RDD[Any] = sc.parallelize(data)
  val result = firstRDD
    //.map( x => x+1)
    //.flatMap(x => List (x, x*2, x*3))
    //.filter(x => x >3 )
    .collect()
    println {
      result.toList
   }
*/
}


