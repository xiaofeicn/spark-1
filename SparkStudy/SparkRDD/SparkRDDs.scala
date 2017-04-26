package SparkTeach.SparkRDD

import java.util
import java.lang.Float

import org.apache.spark.sql.SparkSession

/**
  * Created by admin on 2017/4/24.
  */
object SparkRDDs extends App{
//  transformation,action-->转换算子，提交。
//  val list = new util.ArrayList[String]()
  val spark = SparkSession.builder().appName("RDD").master("local[*]").getOrCreate()
//  val rdd = spark.sparkContext.textFile("RobotClean.txt").map(f=>1).map(f=>"1").map(f=>{1->"1"}).foreach(println) //map算子
//  val rdd = spark.sparkContext.textFile("1.txt").map(f=>f.split("\t")).filter(_.length==6)
//                  .filter(f =>f(1).matches("[0-9].+")).filter(f=>f(2).matches("[0-9]+"))
//                  .filter(f=>new Float(f(1))>9.0 && new Float(f(2))>500000).foreach(f=>println(f.mkString(",")))
  val rdd = spark.sparkContext.textFile("1.txt").flatMap(_.split("\t")).collect()
  println(rdd.length)

}
