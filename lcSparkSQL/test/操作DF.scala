package lcSparkSQL.test

import org.apache.spark.sql.SparkSession

object 操作DF {
  case class Person(name:String,age:Int)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    
    val df = spark.sparkContext.textFile("E:/people.txt").map(_.split(",")).map(m =>Person(m(0),m(1).toInt)).toDF()
    //打印出df的结构图
//    df.printSchema()
    
//    df.select("name").show(5)
    
//    df.filter($"age">20).show(5)
    
    
//    df.groupBy("age").count().show(5)
    
    
  }
}