package streaming

import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("CS422-Project2-Task3") //.setMaster("local[16]")
    val streaming = new SparkStreaming(sparkConf, args)
    val output = "output"

    streaming.consume()
  }
}
