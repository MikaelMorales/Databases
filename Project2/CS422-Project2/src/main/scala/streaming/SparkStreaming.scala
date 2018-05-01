package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class SparkStreaming(sparkConf: SparkConf, args: Array[String]) {

  val sparkconf: SparkConf = sparkConf

  // get the directory in which the stream is filled.
  val inputDirectory: String = args(0)

  // number of seconds per window
  val seconds: Int = args(1).toInt

  // K: number of heavy hitters stored
  val TOPK: Int = args(2).toInt

  // precise Or approx
  val execType = args(3)

  //  create a StreamingContext, the main entry point for all streaming functionality.
  val ssc = new StreamingContext(sparkConf, Seconds(seconds))

  def consume() {

    // create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(inputDirectory)

    // parse the stream. (line -> (IP1, IP2))
    val words = linesDStream.map(x => (x.split("\t")(0), x.split("\t")(1)))

    if (execType.contains("precise")) {
      //TODO : Implement precise calculation
    } else if (execType.contains("approx")) {
      //TODO : Implement approx calculation (you will have to implement the CM-sketch as well
    }

    // Start the computation
    ssc.start()
  
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }
}