package lcSparkSQL.test

import org.apache.spark.sql.SparkSession

object SqlFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    
    //直接使用sql查询文件，不用加载
    spark.sql("select * from parquet.`G:\\spark源码\\examples\\src\\main\\resources\\users.parquet`").show()

    
    
    
    
    
    
  }
}