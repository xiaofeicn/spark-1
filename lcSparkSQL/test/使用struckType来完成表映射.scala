package lcSparkSQL.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.avro.io.Perf.IntTest
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row

object 使用struckType来完成表映射 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    
    val fieldString = "name,age"
    
    val fields = fieldString.split(",")
    println(fields.mkString(";"))
    //将字符串转换成一个StructType类型的，StructType是一种复合型的数据类型。
    val mapField = fields.map(f => StructField(f,StringType,false))
    //因为我们使用的age应该是一个int类型的，所以我们可以自定义一个方法。返回一个StructType
    def costumField(fileds:Array[String]):Array[StructField] = {
      val newFields = ArrayBuffer[StructField]()
      
      newFields.append(StructField(fileds(0),StringType,false))
      newFields.append(StructField(fileds(1),IntegerType,false))
      newFields.toArray
    }
    val selfFields = costumField(fields)
    //以上我们得到一个Array[StructField]的数组
    //创建一个RDD，从一个文本文件中
    val textRDD = spark.sparkContext.textFile("E:/people.txt")
    //将textRDD的每一行分割，并且通过map得到一个RDD[Row]
    val rowRDD = textRDD.map(_.split(",")).map(f =>Row(f(0),f(1).trim().toInt))
    //创建DF
    spark.createDataFrame(rowRDD, StructType(selfFields)).show()
    
    
  
  }
}