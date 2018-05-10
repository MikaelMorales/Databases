package cubeoperator

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val inputFile1= "/Users/Mikael/Documents/Databases/Project2/CS422-Project2/input/lineorder_small.tbl"
    val inputFile2= "/Users/Mikael/Documents/Databases/Project2/CS422-Project2/input/lineorder_medium.tbl"
    val files = List(inputFile1, inputFile2)

    val groupingList1 = List("lo_suppkey")
    val groupingList3 = List("lo_suppkey","lo_shipmode","lo_orderdate")
    val groupingList6 = List("lo_suppkey","lo_shipmode","lo_orderdate", "lo_custkey")
    val dimensions = List(groupingList1, groupingList3, groupingList6)

    val reducers = List(1, 5, 10)
    reducers.foreach(r => {
      files.foreach(inputFile => {
        println(s"*****************BENCHMARK WITH ${inputFile.split("/").last}******************************")
        dimensions.foreach(groupingList => {
          benchmarkCube(sqlContext, inputFile, r, groupingList, "lo_supplycost", "SUM")
        })
      })
    })
  }

  private def benchmarkCube(sqlContext: SQLContext, inputFile: String, reducers: Int, groupingList: List[String], aggAttribute: String, agg: String): Unit = {
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(inputFile)

    val rdd = df.rdd
    val schema = df.schema.toList.map(x => x.name)
    val dataset = new Dataset(rdd, schema)
    val cb = new CubeOperator(reducers)
    var t1 = System.nanoTime
    cb.cube_naive(dataset, groupingList, "lo_supplycost", "SUM").collect()
    var t2 = System.nanoTime
    //res.saveAsTextFile("outputCubeNaive")
    println(s"Naive Cube with $reducers reducers & ${groupingList.size} dimensions: ${(t2-t1)/Math.pow(10,9)} seconds")

    t1 = System.nanoTime
    cb.cube(dataset, groupingList, "lo_supplycost", "SUM").collect()
    t2 = System.nanoTime
    //res.saveAsTextFile("outputCube")
    println(s"Optimized Cube with $reducers reducers & ${groupingList.size} dimensions: ${(t2-t1)/Math.pow(10,9)} seconds")
  }
}