package streaming;

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ SQLContext, Row, DataFrame }
import com.typesafe.config.{ ConfigFactory, Config }
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import java.io._

object Main {
  def main(args: Array[String]) {
    val output = "output"
    val sparkConf = new SparkConf().setAppName("CS422-Project2-Task3") //.setMaster("local[16]")

    val streaming = new SparkStreaming(sparkConf, args);

    streaming.consume();
  }
}
