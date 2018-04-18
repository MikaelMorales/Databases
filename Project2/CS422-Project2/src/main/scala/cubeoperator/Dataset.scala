package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class Dataset(rdd: RDD[Row], schema: List[String]) {
  val this.rdd = rdd
  val this.schema = schema
  
  def getRDD(): RDD[Row] = {
    rdd
  }
  
  def getSchema(): List[String] = {
    schema
  }
}