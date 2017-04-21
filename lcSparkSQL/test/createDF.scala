package lcSparkSQL.test

import org.apache.spark.sql.SparkSession

object createDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    
    //1.ͨ��sparkSession�����һ��DF
    //1.1ͨ��read.format�ƶ��ļ���ʽ
    spark.read.format("text").load("E:/1.txt").show(5)
    //1.2ֱ�Ӷ�ȡtext��ʽ�ĵ�
    spark.read.text("E:/1.txt").show(5)
    
    //2.ͨ��RDDת���õ�
    spark.sparkContext.textFile("E:/1.txt").toDF().show(5)
    
    
    
    
  }
}