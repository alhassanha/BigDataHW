package com.task

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import java.io._

object Main extends App {
  override def main(args: Array[String]): Unit = {
    val variant = 330
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Read Movie Ratings")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("src/resources/u.data")
    val ratings = data.map(
      row => new Rating(row.split("\\t")(0).toInt, row.split("\\t")(1).toInt,
              row.split("\\t")(2).toInt
      )
    )
    val allGroupedRatings = ratings.groupBy(_.rating).mapValues(_.size).sortBy(_._1).map(x => x._2)
    val targetRatings = ratings.filter(_.movie_id==variant).groupBy(_.rating).mapValues(_.size).sortBy(_._1).map(x => x._2)
    val output = Map(
      "hist_film" -> targetRatings.collect().toList,
      "hist_all" -> allGroupedRatings.collect().toList
    )
    val jsonOutput = Json(DefaultFormats).write(output)
    val pw = new PrintWriter(new File("src/resources/output.json"))
    pw.write(jsonOutput)
    pw.close()
    sc.stop()
  }
}
