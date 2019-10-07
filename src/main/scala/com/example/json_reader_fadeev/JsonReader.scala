package com.example.json_reader_fadeev

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s_


object JsonReader extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName( name = "DZ")
    .master( master = "local[*]" )
    .getOrCreate()
  val sc = spark.sparkContext
//  val json = spark.read.json("/home/fadeev/Desktop/winemag-data-130k-v2.json")
//  val lines = sc.textFile("/home/fadeev/Desktop/winemag-data-130k-v2.json")
//  val lines = sc.spark.read.
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
}


