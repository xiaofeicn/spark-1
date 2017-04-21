package lcSparkSQL.test

import org.apache.spark.sql.SparkSession

object LoadAndSave {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    //通过load加载数据文件，默认文件类型为parquet类型
//    spark.read.load("E:/people.txt")//注意这样写是不正确的，因为txt文件不是parquet数据类型，所以我先注释掉了
    
//    spark.read.text("E:/people.txt")//这样就可以了，但是这样没有使用load方法，下面看使用load加载指定数据源类型的方法
    val file = spark.read.format("text").load("E:/people.txt")//这样就指定了我们要load的数据源类型了
    //将file  save到文件系统上
//    file.write.save("E:/x.parquet")//默认格式也是parquet数据类型的
    
    file.write.format("text").save("E:/x")//写时候也可以指定数据源类型
    
   
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
 
}