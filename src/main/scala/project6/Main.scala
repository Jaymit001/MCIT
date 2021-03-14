package project6

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait Main {

  val conf = new Configuration()
  conf.addResource(new Path("/home/jay/opt/hadoop-2.7.7/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/jay/opt/hadoop-2.7.7/etc/cloudera/hdfs-site.xml"))
  val fs:FileSystem = FileSystem.get(conf)
  val spark: SparkSession =
    SparkSession.builder().appName("SparkSQL PROJECT").master("local[*]").getOrCreate()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(10))

  val rootLogger: Logger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

}
