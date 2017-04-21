package lcSparkSQL.test

import org.apache.spark.sql.{SQLContext, SparkSession}
import java.lang.Double


case class Movie(name:String,rating:String,comments_number:String,time:String)  //Ϊ���潨�����ֶΣ������ᱻ���������Ұ�������Ĺ�������������ֶ�
object FromTxtToView {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("DemoTransformations").master("local").getOrCreate() //�������
    val sc = spark.sparkContext  //����spark������
    import spark.implicits._  //������ʽת��
    val file =sc.textFile("D:\\1.csv")  //���ļ�ϵͳ����ļ�����������ΪdataFrame��ע�������ϵͳ�ļ������Ǳ��أ�������HDFS
    
    
    file.map(_.split(",")).filter(f=>{
      f(1).matches("^[0-9].+")}).map(f=>Movie(f(0),f(1),f(2),f(4))).toDF().createOrReplaceTempView("movie")  //��һ���ĵڶ���map����ʹ���������Movie�࣬���ֶν�������
      spark.table("movie").cache() //��movie���������
      
   spark.sql("select name,rating,comments_number,time from movie where rating > 9.0 and comments_number > 500000").createOrReplaceTempView("top10")
   spark.table("top10").cache()  //���������
   spark.sql("select name,max(comments_number) as max from top10 group by name").createOrReplaceTempView("disTop10") //�������ֽ���ȥ��
   spark.table("disTop10").cache() //���������

   val top10 = spark.sql("select t.name,t.rating,t.comments_number,t.time from disTop10 as dt left join top10 as t on dt.max=t.comments_number order by t.comments_number desc")
   top10.show()
   //����ʹ��һ���������������ѯ���������ΪdataFrame
   top10.write.mode("overwrite").format("jdbc")
         .option("url","jdbc:mysql://localhost:3306/phone?serverTimezone=GMT")
         .option("dbtable", "top10")
         .option("user", "root")
         .option("password", "root").save()  //����ѯ����浽���ݿ�
  }
}