package lcSparkSQL.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.avro.io.Perf.IntTest
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row

object ʹ��struckType����ɱ�ӳ�� {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    
    val fieldString = "name,age"
    
    val fields = fieldString.split(",")
    println(fields.mkString(";"))
    //���ַ���ת����һ��StructType���͵ģ�StructType��һ�ָ����͵��������͡�
    val mapField = fields.map(f => StructField(f,StringType,false))
    //��Ϊ����ʹ�õ�ageӦ����һ��int���͵ģ��������ǿ����Զ���һ������������һ��StructType
    def costumField(fileds:Array[String]):Array[StructField] = {
      val newFields = ArrayBuffer[StructField]()
      
      newFields.append(StructField(fileds(0),StringType,false))
      newFields.append(StructField(fileds(1),IntegerType,false))
      newFields.toArray
    }
    val selfFields = costumField(fields)
    //�������ǵõ�һ��Array[StructField]������
    //����һ��RDD����һ���ı��ļ���
    val textRDD = spark.sparkContext.textFile("E:/people.txt")
    //��textRDD��ÿһ�зָ����ͨ��map�õ�һ��RDD[Row]
    val rowRDD = textRDD.map(_.split(",")).map(f =>Row(f(0),f(1).trim().toInt))
    //����DF
    spark.createDataFrame(rowRDD, StructType(selfFields)).show()
    
    
  
  }
}