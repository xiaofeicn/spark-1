package lcSparkStreaming

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Open {
  def main(args: Array[String]): Unit = {
    //新建一个SparkConf
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
    //已上面的SparkConf为参数创建一个StreamingContext,批间隔为2秒
    val ssc = new StreamingContext(conf,Seconds(2))
    //创建一个本地端口，用于接收流数据,这里将map过后的离散流DStream通过制表符分割，然后转换为一个RDD再Action操作打印到控制台
    val ds = ssc.socketTextStream("localhost",9999).flatMap(_.split("\t"))
    val wordsMap = ds.map((_,1))
    wordsMap.reduceByKey(_+_).print()
    //刚开始我以为上面那写步骤之后就可以了，事实证明太天真了，还需要
    ssc.start()
    //开始这个链接任务,然后等待计算结果
    ssc.awaitTermination()
    
    
    
  }
}