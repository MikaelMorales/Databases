package thetajoin

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val reducers = 10
    val maxInput = 1000 
    val inputFile1="/Users/Mikael/Documents/Databases/Project2/CS422-Project2/input/input1_1K.csv"
    val inputFile2="/Users/Mikael/Documents/Databases/Project2/CS422-Project2/input/input2_1K.csv"
    
    val output = "outputThetaJoin"
    
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)   
    
    val df1 = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(inputFile1)
    
    val df2 = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(inputFile2)
    
    val rdd1 = df1.rdd
    val rdd2 = df2.rdd
    
    val schema1 = df1.schema.toList.map(x => x.name)
    val schema2 = df2.schema.toList.map(x => x.name)
    
    val dataset1 = new Dataset(rdd1, schema1)
    val dataset2 = new Dataset(rdd2, schema2)        
    
    val tj = new ThetaJoin(dataset1.getRDD().count, dataset2.getRDD().count, reducers, maxInput)
    val res = tj.theta_join(dataset1, dataset2, "num", "num", "=")

    println(res.count)
    res.coalesce(1).saveAsTextFile(output)
  }     
}
