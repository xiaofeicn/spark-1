package lcSparkSQL.test

import org.apache.spark.sql.SparkSession

object demo {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("a").master("local").getOrCreate()
    var list = List(1,2,3,4,5,6,6)
    print(spark.sparkContext.parallelize(list).reduce(_+_))
    
    
  }
}