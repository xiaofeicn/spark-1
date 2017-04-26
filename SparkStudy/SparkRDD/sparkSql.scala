package SparkTeach.SparkRDD

import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by admin on 2017/4/25.
  */
case class Movie(name: String, grade: String,grade_num:String,url:String,date:String,actor:String)
//schema
object sparkSql  {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SparkSql")
      .config("spark.local.dir", s"D:/spark-local")
      .config("spark.sql.warehouse.dir", s"D:/spark-warehouse")
      .getOrCreate()
    //入口
      import spark.implicits._  //rdd转DF必须导入session的隐式转换。

    //  rdd.createOrReplaceTempView("rdd")
    //  val resualtSet = spark.sql("select * from rdd where")
    //  resualtSet.show()//  val list=Array((1,1),(1,1),(1,1),(1,1),(1,1))
    //  val rdd = spark.sparkContext.parallelize(list).toDF()
    val rdd = spark.sparkContext.textFile("1.txt").map(_.split("\t")).filter(f=>f.length==6)
      .map(f=>new Movie(f(0),f(1),f(2),f(3),f(4),f(5))) //从fs上读取文件，并且使用case类来映射字段

    val dfFromRdd = rdd.toDF()
    dfFromRdd.createOrReplaceTempView("AllMovie")
    spark.table("AllMovie").cache()
    spark.sql("select * from AllMovie where grade>9.0 and grade_num>500000").createOrReplaceTempView("Top10_first")
    spark.table("Top10_first").cache()
    spark.sql("select name,max(grade) as grade,max(grade_num) as grade_num from Top10_first group by name" +
      " order by grade_num desc").createOrReplaceTempView("top10")

    val p = new Properties()
    p.setProperty("user","root")
    p.setProperty("password","root") //配置Properties设置，主要是用户名和密码

//    val df = spark.read.format("text").load("1.txt")
    //    df.show()
//    val jdbcDF = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/phone")
//      .option("dbtable","allphone").option("user","root")
//      .option("password","root").load()
//    jdbcDF.show()  //使用jdbc来读取mysql表，获取DF


    spark.table("top10").write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/movie","top10",p) //使用write的jdbc往mysql中写数据
//    top10.write.mode("overwrite").format("jdbc")
//      .option("url","jdbc:mysql://localhost:3306/phone?serverTimezone=GMT")
//      .option("dbtable", "top10")
//      .option("user", "root")
//      .option("password", "root").save()  //将查询结果存到数据库 eclipse中可以写入数据库，而idea不能写。不知道为什么


  }
}
