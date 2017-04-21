package lcSparkSQL.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object GetDF {
  def main(args: Array[String]): Unit = {
    case class Person(name:String,age:Int)
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    val people = sc.textFile("E:/people.txt").map(f => f.split(",")).map(u => new Person(u(0),u(1).trim().toInt))
    
    
    
    
  }
}