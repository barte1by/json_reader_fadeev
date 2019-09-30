package com.example.json_reader_fadeev

import org.apache.spark.sql.SparkSession

object JsonReader extends App {
  val spark: SparkSession = SparkSession.builder().appName( name = "DZ").getOrCreate()
  val sc = spark.sparkContext
  val json = spark.read.json("/home/fadeev/Desktop/winemag-data-130k-v2.json")
  val lines = sc.textFile("/home/fadeev/Desktop/winemag-data-130k-v2.json")
  //val lines = sc.spark.read.
  json.show()
}
