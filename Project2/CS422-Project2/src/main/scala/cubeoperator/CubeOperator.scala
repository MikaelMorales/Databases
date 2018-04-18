package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    val mapReduceResult = agg match {
      case "AVG" =>
        val sum = mapReduce(rdd, index, indexAgg, "SUM")
        val count = mapReduce(rdd, index, indexAgg, "COUNT")
        sum.join(count).mapValues(t => t._1 / t._2)
      case _ => mapReduce(rdd, index, indexAgg, agg)
    }

    convertKeyToString(mapReduceResult)
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {
    //TODO naive algorithm for cube computation
    null
  }

  def mapReduce(rdd: RDD[Row], index: List[Int], indexAgg: Int, agg: String): RDD[(List[Any], Double)] = {
    val myMap = mMap(rdd,index,indexAgg,agg)
    val mapReduceResult = myMap.groupByKey().mapValues(f => agg match {
      case "COUNT" => f.sum
      case "SUM" => f.sum
      case "MIN" => f.min
      case "MAX" => f.max
    })

//   val mapResult = mMap(rdd,index,indexAgg,agg)
//   val combineResult = combine(mapResult, agg)
//   val mapReduceResult = reduce(combineResult, agg)
   
   val res = mapReduceResult.flatMap(t => {
     val c = t._1.foldLeft(List(Nil): List[List[Any]])((acc, l) => {
       var newRes: List[List[Any]] = List.empty
       acc.foreach { r => 
         newRes = newRes :+ (r :+ "*") :+ (r :+ l)
       }
       newRes
     })
     c.map(u => (u, t._2))
   })
 
   res.groupByKey().map(t => agg match {
     case "COUNT" => (t._1, t._2.sum)
     case "SUM" => (t._1, t._2.sum)
     case "MIN" => (t._1, t._2.min)
     case "MAX" => (t._1, t._2.max)
   })
  }

  def mMap(rdd: RDD[Row], index: List[Int], indexAgg: Int, agg: String): RDD[(List[Any], Double)] = {
    val res = rdd.map(row => (index.map(i => row.get(i)), row.getInt(indexAgg)))
    res.mapValues( t => agg match {
      case "COUNT" => 1.0
      case "SUM" => t.toDouble
      case "MIN" => t.toDouble
      case "MAX" => t.toDouble
      case _ => throw new IllegalArgumentException(s"Invalid Aggregation Operator: $agg")
    })
  }

  def combine(rdd: RDD[(List[Any], Double)], agg: String): RDD[(List[Any], List[Double])] = {
    val initialValue = (x: Double) => x :: Nil
    val globalCombiner = (x: List[Double], y: List[Double]) => x ++ y
    val localCombiner = (x: List[Double], y: Double) => agg match {
      case "COUNT" => (x.head + y) :: Nil
      case "SUM" => (x.head + y) :: Nil
      case "MIN" => if (x.head < y) x else y :: Nil
      case "MAX" => if (x.head < y) y :: Nil else x
    }

    rdd.combineByKey(initialValue, localCombiner, globalCombiner)
  }

  def reduce(rdd: RDD[(List[Any], List[Double])], agg: String): RDD[(List[Any], Double)] = {
    val localAggregate = (init: Double, v: List[Double]) => agg match {
      case "COUNT" => v.sum
      case "SUM" => v.sum
      case "MIN" => v.min
      case "MAX" => v.max
    }
    val merge = (u: Double, v: Double) => agg match {
      case "COUNT" => u + v
      case "SUM" => u + v
      case "MIN" => if (u < v) u else v
      case "MAX" => if (u < v) v else u
    }

    rdd.aggregateByKey(0.0)(localAggregate, merge)
  }

  def convertKeyToString(rdd: RDD[(List[Any], Double)]): RDD[(String, Double)] = {
    rdd.map(t => {
      val s = t._1.foldLeft("")((acc,b) => {
        if (!acc.isEmpty())
          acc + "," + b.toString()
        else
          b.toString()
      })
      (s, t._2)
    })
  }
}
