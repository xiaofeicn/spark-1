package lcSparkSQL.test

import org.apache.spark.sql.SparkSession

object SqlFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    
    //ֱ��ʹ��sql��ѯ�ļ������ü���
    spark.sql("select * from parquet.`G:\\sparkԴ��\\examples\\src\\main\\resources\\users.parquet`").show()

    
    
    
    
    
    
  }
}