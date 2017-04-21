package lcSparkSQL.test

import org.apache.spark.sql.SparkSession

object FromJson {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    val json = spark.read.json("G:\\sparkԴ��\\examples\\src\\main\\resources\\people.json")
    json.printSchema()
    
    
    
    
    
  }
}