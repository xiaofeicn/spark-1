package lcSparkSQL.test

import org.apache.spark.sql.SparkSession

object LoadAndSave {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    import spark.implicits._
    //ͨ��load���������ļ���Ĭ���ļ�����Ϊparquet����
//    spark.read.load("E:/people.txt")//ע������д�ǲ���ȷ�ģ���Ϊtxt�ļ�����parquet�������ͣ���������ע�͵���
    
//    spark.read.text("E:/people.txt")//�����Ϳ����ˣ���������û��ʹ��load���������濴ʹ��load����ָ������Դ���͵ķ���
    val file = spark.read.format("text").load("E:/people.txt")//������ָ��������Ҫload������Դ������
    //��file  save���ļ�ϵͳ��
//    file.write.save("E:/x.parquet")//Ĭ�ϸ�ʽҲ��parquet�������͵�
    
    file.write.format("text").save("E:/x")//дʱ��Ҳ����ָ������Դ����
    
   
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
 
}