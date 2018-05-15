package thetajoin

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class ThetaJoin(numR: Long, numS: Long, reducers: Int, bucketsize: Int) extends java.io.Serializable {
  case class Region(rowS: Long, rowF: Long, colS: Long, colF: Long)

  val logger: Logger = LoggerFactory.getLogger("ThetaJoin")
  
  // Random samples for each relation
  // helper structures, you are allowed
  // not to use them
  var horizontalBoundaries: Array[Int] = Array[Int]()
  var verticalBoundaries: Array[Int] = Array[Int]()
  
  // Number of values that fall in each partition
  // helper structures, you are allowed
  // not to use them
  var horizontalCounts: Array[Int] = Array[Int]()
  var verticalCounts: Array[Int] = Array[Int]()
  
  /*
   * This method gets as input two datasets and the condition
   * and returns an RDD with the result by projecting only 
   * attr1 and attr2
   * You are not allowed to change the definition of this function.
   * */  
  def theta_join(dataset1: Dataset, dataset2: Dataset, attr1:String, attr2:String, op:String): RDD[(Int, Int)] = {
    val schema1 = dataset1.getSchema()
    val schema2 = dataset2.getSchema()
    
    val rdd1 = dataset1.getRDD()
    val rdd2 = dataset2.getRDD()
    
    val index1 = schema1.indexOf(attr1)
    val index2 = schema2.indexOf(attr2)        
    
    // Sample both datasets
    val fraction = 1.0 / Math.sqrt(numS*numR/reducers)
    horizontalBoundaries = initializeBoundaries(rdd2, index2, fraction)
    verticalBoundaries = initializeBoundaries(rdd1, index1, fraction)
    verticalCounts = initializeCount(rdd1, index1, verticalBoundaries)
    horizontalCounts = initializeCount(rdd2, index2, horizontalBoundaries)

    val buckets = M_Bucket_I(bucketsize, op).zipWithIndex
    val sortedR = rdd1.map(r => r.getInt(index1)).sortBy(identity)
    val sortedS = rdd2.map(r => r.getInt(index2)).sortBy(identity)

    val rWithBuckets = sortedR.zipWithIndex().flatMap { case (v,i) =>
      val b = buckets.filter{ case (region, _) => region.rowS <= i && i <= region.rowF }
      b.map { case (_, bucketNb) => (bucketNb,v) }
    }

    val sWithBuckets = sortedS.zipWithIndex().flatMap { case (v,i) =>
      val b = buckets.filter{ case (region, _) => region.colS <= i && i <= region.colF }
      b.map { case (_, bucketNb) => (bucketNb,v) }
    }

    val partitionedR = rWithBuckets.partitionBy(new HashPartitioner(reducers))
    val partitionedS = sWithBuckets.partitionBy(new HashPartitioner(reducers))

    partitionedR.zipPartitions(partitionedS){ case (i1, i2) => local_thetajoin(i1, i2, op)}
  }

  private def initializeBoundaries(rdd: RDD[Row], index: Int, fraction: Double): Array[Int] = {
    val samples = rdd.map(r => r.getInt(index)).distinct().sample(withReplacement = false, fraction).sortBy(identity).collect()

    val boundaries = new Array[Int](samples.length + 2)
    Array.copy(samples, 0, boundaries, 1, samples.length)
    boundaries(0) = Int.MinValue
    boundaries(boundaries.length-1) = Int.MaxValue
    boundaries
  }

  private def initializeCount(rdd: RDD[Row], index: Int, boundaries: Array[Int]): Array[Int] = {
    val sums = rdd.map { r =>
      val value = r.getInt(index)
      boundaries.indexWhere(v => value <= v)
    }.countByValue()

    val counts = new Array[Int](boundaries.length)
    sums.foreach { case (i, value) => counts(i) = value.toInt }
    counts
  }

  private def M_Bucket_I(bucketSize: Int, op:String): Array[Region] = {
    var row = 0L
    val res = new ArrayBuffer[Region]()
    while (row < numR) {
      val regions = coverSubMatrix(row, bucketsize, op)
      if (regions != null) {
        row = regions.head.rowF + 1
        res.appendAll(regions)
      } else {
        row += bucketSize
      }
    }
    res.toArray
  }

  private def coverSubMatrix(row: Long, bucketSize: Int, op:String): Array[Region] = {
    var maxScore = 0L
    var bestRegions: Array[Region] = null
    var i = 0
    while (i < bucketSize) {
      if(row + i < numR && (i == 0 || bucketSize % i == 0)) {
        val (regions, totalCandidatesCells) = coverRows(row, row + i, bucketSize, op)
        val score = if (regions.length == 0) -1 else totalCandidatesCells / regions.length
        if (score >= maxScore) {
          maxScore = score
          bestRegions = regions
        }
      }

      i += 1
    }

    bestRegions
  }

  private def coverRows(rowF: Long, rowL: Long, bucketSize: Int, op:String): (Array[Region], Long) = {
    var totalCandidateCells = 0L
    var startingCol = 0L
    var endingCol = 0L

    val columnLength = rowL-rowF+1
    var currentCells = 0L

    val regions = new ArrayBuffer[(Long, Long)]()
    val vertical = getVerticalBoundaryIndexes(rowF,rowL)

    var c = 0L
    while (c < numS) {
      val b = getBoundaryIndex(c, isHorizontal = true)
      val numberCandidateCells = vertical.iterator.map { v =>
        val valid = checkIfValidBoundaries(v, b, op)
        if (valid) verticalCounts(v._2) else 0
      }.sum
      if (numberCandidateCells > 0) {
        if (currentCells + columnLength > bucketSize) {
          regions += ((startingCol, c-1))
          startingCol = c
          currentCells = 0
        }
        currentCells += columnLength
        totalCandidateCells += numberCandidateCells
        endingCol = c
      }
      c += 1
    }

    if (currentCells > 0) {
      regions.append((startingCol, endingCol))
    }

    val mRegions = regions.toArray.map { case (cS, cL) => Region(rowF, rowL, cS, cL)}
    (mRegions, totalCandidateCells)
  }

  private def checkIfValidBoundaries(indexR: (Int, Int), indexS: (Int, Int), op:String): Boolean = {
    val Rl = verticalBoundaries(indexR._1)
    val Ru = verticalBoundaries(indexR._2)
    val Sl = horizontalBoundaries(indexS._1)
    val Su = horizontalBoundaries(indexS._2)
    op match {
      case "<" => Rl < Su
      case "<=" => Rl <= Su
      case "=" => (Rl >= Sl && Rl <= Su) || Ru >= Sl
      case "!=" => Rl != Sl || Ru != Sl || Rl != Ru
      case ">" => Ru > Sl
      case ">=" => Ru >= Sl
    }
  }

  private def getVerticalBoundaryIndexes(rowStart: Long, rowEnd: Long): List[(Int, Int)] = {
    val (lb1, ub1) = getBoundaryIndex(rowStart, isHorizontal = false)
    val (lb2, ub2) = getBoundaryIndex(rowEnd, isHorizontal = false)

    (lb1 to lb2).zip(ub1 to ub2).toList
  }

  private def getBoundaryIndex(r: Long, isHorizontal: Boolean) = {
    val i = r + 1
    var count = if (isHorizontal) horizontalCounts.head else verticalCounts.head
    var index = 0
    while (count < i) {
      index += 1
      count += (if (isHorizontal) horizontalCounts(index) else verticalCounts(index))
    }

    (index - 1, index)
  }

  /*
   * This method takes as input two lists of values that belong to the same partition
   * and performs the theta join on them. Both datasets are lists of tuples (Int, Int)
   * where ._1 is the partition number and ._2 is the value. 
   * Of course you might change this function (both definition and body) if it does not 
   * fit your needs :)
   * */  
  def local_thetajoin(dat1:Iterator[(Int, Int)], dat2:Iterator[(Int, Int)], op:String) : Iterator[(Int, Int)] = {
    val res = new ListBuffer[(Int, Int)]()
    val dat2L = dat2.toList

    for (row1 <- dat1) {
      for (row2 <- dat2L) {
        if (row1._1 == row2._1 && checkCondition(row1._2, row2._2, op))
          res += ((row1._2, row2._2))
      }
    }

    res.toList.iterator
  }  
  
  def checkCondition(value1: Int, value2: Int, op:String): Boolean = {
    op match {
      case "=" => value1 == value2
      case "<" => value1 < value2
      case "<=" => value1 <= value2
      case ">" => value1 > value2
      case ">=" => value1 >= value2
      case "!=" => value1 != value2
    }
  }
}