package streaming;

import scala.io.Source
import scala.util.control._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming._
import scala.collection.Seq
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.Strategy
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.spark.util.sketch.CountMinSketch

class SparkStreaming(sparkConf: SparkConf, args: Array[String]) {

  val sparkconf = sparkConf;

  // get the directory in which the stream is filled.
  val inputDirectory = args(0)

  // number of seconds per window
  val seconds = args(1).toInt;

  // K: number of heavy hitters stored
  val TOPK = args(2).toInt;

  // precise Or approx
  val execType = args(3);

  //  create a StreamingContext, the main entry point for all streaming functionality.
  val ssc = new StreamingContext(sparkConf, Seconds(seconds));

  def consume() {

    // create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(inputDirectory);

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