package lcSparkSQL.test

import org.apache.spark.sql.{SQLContext, SparkSession}
import java.lang.Double


case class Movie(name:String,rating:String,comments_number:String,time:String)  //为下面建表创建字段，这个类会被读到，并且按照里面的构造参数构建表字段
object FromTxtToView {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("DemoTransformations").master("local").getOrCreate() //建立入口
    val sc = spark.sparkContext  //建立spark上下文
    import spark.implicits._  //导入隐式转换
    val file =sc.textFile("D:\\1.csv")  //从文件系统里读文件，返回类型为dataFrame。注意这里的系统文件不仅是本地，还包括HDFS
    
    
    file.map(_.split(",")).filter(f=>{
      f(1).matches("^[0-9].+")}).map(f=>Movie(f(0),f(1),f(2),f(4))).toDF().createOrReplaceTempView("movie")  //这一步的第二个map就是使用了上面的Movie类，将字段建立起来
      spark.table("movie").cache() //给movie建立缓存点
      
   spark.sql("select name,rating,comments_number,time from movie where rating > 9.0 and comments_number > 500000").createOrReplaceTempView("top10")
   spark.table("top10").cache()  //建立缓存点
   spark.sql("select name,max(comments_number) as max from top10 group by name").createOrReplaceTempView("disTop10") //跟据名字进行去重
   spark.table("disTop10").cache() //建立缓存点

   val top10 = spark.sql("select t.name,t.rating,t.comments_number,t.time from disTop10 as dt left join top10 as t on dt.max=t.comments_number order by t.comments_number desc")
   top10.show()
   //哎，使用一个变量接受这个查询结果，类型为dataFrame
   top10.write.mode("overwrite").format("jdbc")
         .option("url","jdbc:mysql://localhost:3306/phone?serverTimezone=GMT")
         .option("dbtable", "top10")
         .option("user", "root")
         .option("password", "root").save()  //将查询结果存到数据库
  }
}