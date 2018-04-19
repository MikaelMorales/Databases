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
        val sum = twoPassMapReduce(rdd, index, indexAgg, "SUM")
        val count = twoPassMapReduce(rdd, index, indexAgg, "COUNT")
        sum.join(count).mapValues(t => t._1 / t._2)
      case _ => twoPassMapReduce(rdd, index, indexAgg, agg)
    }

    convertKeyToString(mapReduceResult)
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {
    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    val result = agg match {
      case "AVG" =>
        val sumComb = combinations(mapPhase(rdd, index, indexAgg, "SUM"))
        val sum = combineAndReduce(sumComb, "SUM")
        val countComb = combinations(mapPhase(rdd, index, indexAgg, "COUNT"))
        val count = combineAndReduce(countComb, "COUNT")
        sum.join(count).mapValues(t => t._1 / t._2)
      case _ =>
        val comb = combinations(mapPhase(rdd, index, indexAgg, agg))
        combineAndReduce(comb, agg)
    }

    convertKeyToString(result)
  }

  def twoPassMapReduce(rdd: RDD[Row], index: List[Int], indexAgg: Int, agg: String): RDD[(List[Any], Double)] = {
    // Alternative implementation
    //   val combineResult = combine(mapPhase(rdd,index,indexAgg,agg), agg)
    //   val mapReduceResult = reduce(combineResult, agg)

    /** MapReduce first pass */
    val mapReduceResult = combineAndReduce(mapPhase(rdd,index,indexAgg,agg), agg)

    /** Generate combinations */
    val comb = combinations(mapReduceResult)

    /** MapReduce second pass with combinations */
    combineAndReduce(comb, agg)
  }

  def combinations(rdd: RDD[(List[Any], Double)]): RDD[(List[Any], Double)] = {
    rdd.flatMap(t =>
      t._1.foldLeft(List((Nil, t._2)): List[(List[Any], Double)])((b, l) =>
        b.map(x => (x._1 :+ "*", x._2)) ++ b.map(x => (x._1 :+ l, x._2)))
    )
  }

  def mapPhase(rdd: RDD[Row], index: List[Int], indexAgg: Int, agg: String): RDD[(List[Any], Double)] = {
    val res = rdd.map(row => (index.map(i => row.get(i)), row.getInt(indexAgg)))
    res.mapValues( t => agg match {
      case "COUNT" => 1.0
      case "SUM" => t.toDouble
      case "MIN" => t.toDouble
      case "MAX" => t.toDouble
      case _ => throw new IllegalArgumentException(s"Invalid Aggregation Operator: $agg")
    })
  }

  def combineAndReduce(rdd: RDD[(List[Any], Double)], agg: String): RDD[(List[Any], Double)] = {
    rdd.groupByKey().mapValues(t => agg match {
      case "COUNT" => t.sum
      case "SUM" => t.sum
      case "MIN" => t.min
      case "MAX" => t.max
    })
  }

  /**
    * Utility method to convert a List[Any] to a string
    * @param rdd RDD of tuples
    * @return An RDD with the key converted to string
    */
  def convertKeyToString(rdd: RDD[(List[Any], Double)]): RDD[(String, Double)] = {
    rdd.map(t => {
      val s = t._1.foldLeft("")((acc,b) => {
        if (!acc.isEmpty)
          acc + "," + b.toString
        else
          b.toString
      })
      (s, t._2)
    })
  }

  /**************************************************************************
    * Alternative implementation of reduce and combine that shows every step
    * of the algorithm as shown in the project statement
    *************************************************************************/
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
    val localAggregate = (_: Double, v: List[Double]) => agg match {
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
}