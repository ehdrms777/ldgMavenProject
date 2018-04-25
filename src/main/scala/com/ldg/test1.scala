package com.ldg

import org.apache.spark.sql.SparkSession

object test1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ldgProject").
      config("spark.master", "local").
      getOrCreate()
    println("spark test")
  }
}
