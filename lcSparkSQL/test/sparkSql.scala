package lcSparkSQL.test

import scala.tools.scalap.Main
import org.apache.spark.sql.SparkSession

object sparkSql {
  case class Person(name:String,age:Int)
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    
//    val df = spark.sparkContext.textFile("E:/people.txt").map(_.split(",")).map(f=>Person(f(0),f(1).toInt)).toDF()
//    
//    df.createOrReplaceTempView("people")
//    
//    spark.sql("select * from people").show(5)
    spark.read.text("E:/people.txt")
    
    
    
    
    
    
    
  }
  
  
  
}