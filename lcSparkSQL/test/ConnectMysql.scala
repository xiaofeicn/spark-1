package lcSparkSQL.test

import org.apache.spark.sql.SparkSession
import com.mysql.jdbc.Driver
object ConnectMysql {
  def main(args: Array[String]): Unit = {
   
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val mysql = spark.read.format("jdbc")
                .option("url","jdbc:mysql://localhost:3306/phone")
                .option("dbtable", "allphone")
                .option("user","root")
                .option("password","root").load()
    mysql.show(5)
  }
}