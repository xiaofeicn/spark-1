package lcSparkStreaming

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Open {
  def main(args: Array[String]): Unit = {
    //�½�һ��SparkConf
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
    //�������SparkConfΪ��������һ��StreamingContext,�����Ϊ2��
    val ssc = new StreamingContext(conf,Seconds(2))
    //����һ�����ض˿ڣ����ڽ���������,���ｫmap�������ɢ��DStreamͨ���Ʊ���ָȻ��ת��Ϊһ��RDD��Action������ӡ������̨
    val ds = ssc.socketTextStream("localhost",9999).flatMap(_.split("\t"))
    val wordsMap = ds.map((_,1))
    wordsMap.reduceByKey(_+_).print()
    //�տ�ʼ����Ϊ������д����֮��Ϳ����ˣ���ʵ֤��̫�����ˣ�����Ҫ
    ssc.start()
    //��ʼ�����������,Ȼ��ȴ�������
    ssc.awaitTermination()
    
    
    
  }
}