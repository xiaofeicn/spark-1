package lcSparkSQL.test

import org.apache.spark.sql.SparkSession

object hiveOnSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    //µº»Îspark.sql
    import spark.sql
    
    sql("create table sc (name string,age string) row format delimited fields terminated by ','")
    sql("load data local inpath 'E:\\people.txt' into table sc")
    sql("select * from sc").show()
    
    
    
    
    
    
    
    
    
    
    
    
  }
  
  
  
  
  
  
  
  
}