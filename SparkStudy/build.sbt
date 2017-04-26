name := "SparkStudy"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.35"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.5"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.5"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.7"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0"
libraryDependencies += "net.liftweb" % "lift-json_2.11" % "2.6"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.0"
libraryDependencies += "org.apache.hbase" % "hbase" % "1.2.5"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.5"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.5"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.5"
libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.5"

    