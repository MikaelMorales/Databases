package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class ThetaJoinTest extends FlatSpec {
  val sparkConf: SparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
  val ctx: SparkContext = new SparkContext(sparkConf)
  val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(ctx)

  test()

  def test() {
    val reducers = 10
    val maxInput = 1000

    //val inputFile1="input1_1K.csv"
    //val inputFile2="input2_1K.csv"

    //val input1 = new File(getClass.getResource(inputFile1).getFile).getPath
    //val input2 = new File(getClass.getResource(inputFile2).getFile).getPath

    val input1="/Users/Mikael/Documents/Databases/Project2/CS422-Project2/input/input1_1K.csv"
    val input2="/Users/Mikael/Documents/Databases/Project2/CS422-Project2/input/input2_1K.csv"

    val output = "outputTest"

    val df1 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(input1)

    val df2 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(input2)

    val rdd1 = df1.rdd
    val rdd2 = df2.rdd

    val schema1 = df1.schema.toList.map(x => x.name)
    val schema2 = df2.schema.toList.map(x => x.name)

    val dataset1 = new Dataset(rdd1, schema1)
    val dataset2 = new Dataset(rdd2, schema2)

    test(dataset1, dataset2, reducers, maxInput, "=", (x, y) => x == y, schema1, schema2, rdd1, rdd2)
    test(dataset1, dataset2, reducers, maxInput, "<=", (x, y) => x <= y, schema1, schema2, rdd1, rdd2)
    test(dataset1, dataset2, reducers, maxInput, ">=", (x, y) => x >= y, schema1, schema2, rdd1, rdd2)
    test(dataset1, dataset2, reducers, maxInput, "!=", (x, y) => x != y, schema1, schema2, rdd1, rdd2)
    test(dataset1, dataset2, reducers, maxInput, "<", (x, y) => x < y, schema1, schema2, rdd1, rdd2)
    test(dataset1, dataset2, reducers, maxInput, ">", (x, y) => x > y, schema1, schema2, rdd1, rdd2)
  }

  private def test(dataset1: Dataset, dataset2: Dataset, reducers: Int, maxInput: Int, op: String, cond: (Int, Int) => Boolean, schema1: List[String], schema2: List[String], rdd1: RDD[Row], rdd2: RDD[Row]) = {
    val t1 = System.nanoTime
    val resultSize = getTestedImplementationResult(dataset1, dataset2, reducers, maxInput, op)
    val t2 = System.nanoTime

    println((t2-t1)/Math.pow(10,9))

    val index1 = schema1.indexOf("num")
    val index2 = schema2.indexOf("num")

    val resultSizeCartesian = getReferenceImplementationResult(rdd1, rdd2, index1, index2, cond)

    assert(resultSize === resultSizeCartesian)
  }

  private def getTestedImplementationResult(dataset1: Dataset, dataset2: Dataset, reducers: Int, maxInput: Int, op: String): Long = {
    val tj = new ThetaJoin(dataset1.getRDD().count, dataset2.getRDD().count, reducers, maxInput)
    val res = tj.theta_join(dataset1, dataset2, "num", "num", op)

    res.count
  }

  private def getReferenceImplementationResult(rdd1: RDD[Row], rdd2: RDD[Row], index1: Int, index2: Int, cond: (Int, Int) => Boolean): Long = {
    val cartRes = rdd1.cartesian(rdd2).flatMap(x => {
      val v1 = x._1(index1).asInstanceOf[Int]
      val v2 = x._2(index2).asInstanceOf[Int]
      if(cond(v1, v2))
        List((v1, v2))
      else
        List()
    })

    cartRes.count
  }
}