package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

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
  private val seenCMSKeys = new mutable.HashSet[(String, String)]()
  private val globalExact = new mutable.HashMap[(String, String), Long]()

  def consume() {
    // Create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(inputDirectory)

    // Parse the stream. (line -> (IP1, IP2))
    val words = linesDStream.map(x => (x.split("\t")(0), x.split("\t")(1)))

    if (execType.contains("precise")) {
      words.foreachRDD { rdd =>
        val batch = rdd.map(x => (x, 1L)).reduceByKey(_+_)
        val partialMap = batch.collect().toMap
        val localTopK = batch.map{ case (ips, count) => (count, ips) }.sortByKey(ascending = false).take(TOPK)
        partialMap.foreach { case (key, value) =>
            globalExact.put(key, globalExact.getOrElse(key, 0L) + value)
        }
        val globalTopK = globalExact.toSeq.map{ case (ips, count) => (count, ips) }.sortBy(t => -t._1).take(TOPK)
        if (localTopK.nonEmpty && globalTopK.nonEmpty) {
          println("This batch: " + localTopK.mkString("[", ",", "]"))
          println("Global: " + globalTopK.mkString("[", ",", "]"))
        }
      }
    } else if (execType.contains("approx")) {
      words.foreachRDD { rdd =>
        val batch = rdd.map(x => (x, 1L)).reduceByKey(_+_).collect()
        val localCMS = new CountMinSketch(delta, eps)
        batch.foreach { case (k, w) =>
          localCMS.update(k, w)
          globalCMS.update(k, w)
          seenCMSKeys.add(k)
        }
        val localTopK = batch.map{ case (k,_) => (localCMS.get(k), k) }.sortBy(t => -t._1).take(TOPK)
        val globalTopK = seenCMSKeys.toSeq.map{ k => (globalCMS.get(k), k) }.sortBy(t => -t._1).take(TOPK)
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