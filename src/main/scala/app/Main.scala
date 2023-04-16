package app

import app._
import app.recommender.TitlesLoader
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
  }
}
