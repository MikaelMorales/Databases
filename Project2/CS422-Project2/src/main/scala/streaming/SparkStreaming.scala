package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class SparkStreaming(sparkConf: SparkConf, args: Array[String]) {
  // Get the directory in which the stream is filled.
  val inputDirectory: String = args(0)

  // number of seconds per window
  val seconds: Int = args(1).toInt

  // K: number of heavy hitters stored
  val TOPK: Int = args(2).toInt

  // precise Or approx
  val execType = args(3)

  // Default values for Count-Min Sketch
  val delta: Double = if (args.length < 5) 1E-3 else args(4).toDouble
  val eps: Double = if (args.length < 6) 0.01 else args(5).toDouble

  //  create a StreamingContext, the main entry point for all streaming functionality.
  val ssc = new StreamingContext(sparkConf, Seconds(seconds))

  private val globalCMS = new CountMinSketch(delta, eps)
  private val globalExact = scala.collection.mutable.HashMap[(String, String), Long]()

  def consume() {
    // create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(inputDirectory)

    // parse the stream. (line -> (IP1, IP2))
    val words = linesDStream.map(x => (x.split("\t")(0), x.split("\t")(1)))

    if (execType.contains("precise")) {
      words.map(x => (x, 1L)).reduceByKey(_+_).foreachRDD { rdd =>
        val partialMap = rdd.collect().toMap
        val localTopK = rdd.map { case (ips, count) => (count, ips) }.sortByKey(ascending = false).take(TOPK)
        partialMap.foreach { case (key, value) =>
            globalExact.put(key, globalExact.getOrElse(key, 0L) + value)
        }
        val globalTopK = globalExact.toSeq.map { case (ips, count) => (count, ips) }.sortBy(t => -t._1).take(TOPK)
        if (localTopK.nonEmpty && globalTopK.nonEmpty) {
          println("This batch: " + localTopK.mkString("[", ",", "]"))
          println("Global: " + globalTopK.mkString("[", ",", "]"))
        }
      }
    } else if (execType.contains("approx")) {

      words.map(x => (x, 1L)).reduceByKey(_+_).foreachRDD { rdd =>
        val localCMS = new CountMinSketch(delta, eps)
        rdd.foreach { case (k,_) =>
          localCMS.update(k)
          globalCMS.update(k)
        }
        val localTopK = rdd.map{case (k,_) => (localCMS.get(k), k)}.sortByKey(ascending = false).take(TOPK)
        val globalTopK = rdd.map{case (k,_) => (globalCMS.get(k), k)}.sortByKey(ascending = false).take(TOPK)
        if (localTopK.nonEmpty && globalTopK.nonEmpty) {
          println("This batch: " + localTopK.mkString("[", ",", "]"))
          println("Global: " + globalTopK.mkString("[", ",", "]"))
        }
      }
    }

    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }
}