package lcSparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object countByValueTransfomation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(countByValueTransfomation.getClass.toString()).setMaster("local[*]")
    
    val ssc = new StreamingContext(conf,Seconds(5))
    val getDs = ssc.socketTextStream("localhost", 9999)
    getDs.flatMap(_.split(" ")).countByValue(1).print()
    
    val getDs2 = getDs.window(Seconds(60),Seconds(5))
    
    getDs2.transform(rdd =>{
      
      rdd.map(f => (f,1))
      
    })
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}
