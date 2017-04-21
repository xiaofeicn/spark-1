package lcSparkSQL.test

import org.apache.spark.sql.SparkSession

object createDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    
    //1.通过sparkSession来获得一个DF
    //1.1通过read.format制定文件格式
    spark.read.format("text").load("E:/1.txt").show(5)
    //1.2直接读取text格式文档
    spark.read.text("E:/1.txt").show(5)
    
    //2.通过RDD转换得到
    spark.sparkContext.textFile("E:/1.txt").toDF().show(5)
    
    
    
    
  }
}