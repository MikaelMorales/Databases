package thetajoin

import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer


class ThetaJoin(numR: Long, numS: Long, reducers: Int, bucketsize: Int) extends java.io.Serializable {

  case class Region(rowS: Long, rowF: Long, colS: Long, colF: Long)

  val logger: Logger = LoggerFactory.getLogger("ThetaJoin")
  
  // random samples for each relation
  // helper structures, you are allowed
  // not to use them
  var horizontalBoundaries: Array[Int] = Array[Int]()
  var verticalBoundaries: Array[Int] = Array[Int]()
  
  // number of values that fall in each partition
  // helper structures, you are allowed
  // not to use them
  var horizontalCounts: Array[Int] = Array[Int]()
  var verticalCounts: Array[Int] = Array[Int]()
  
  /*
   * this method gets as input two datasets and the condition
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
    
    // Implements the algorithm
    val cs = numS / Math.sqrt(numS*numR*reducers)
    val cr = numR / Math.sqrt(numS*numR*reducers)
    horizontalBoundaries = rdd2.map(r => r.getInt(index2)).distinct().sample(withReplacement = false, cs).sortBy(identity).collect()
    verticalBoundaries = rdd1.map(r => r.getInt(index1)).distinct().sample(withReplacement = false, cr).sortBy(identity).collect()

    horizontalBoundaries +:= Int.MinValue
    horizontalBoundaries :+= Int.MaxValue
    verticalBoundaries +:= Int.MinValue
    verticalBoundaries :+= Int.MaxValue
    horizontalCounts = new Array[Int](horizontalBoundaries.length)
    verticalCounts = new Array[Int](verticalBoundaries.length)

    val vSums = rdd1.map(r => {
      val value = r.getInt(index1)
      val res = verticalBoundaries.zipWithIndex.find(v => value <= v._1)
      (res.get._2, 1)
    }).groupByKey().mapValues(t => t.sum).collect()
    vSums.foreach { case (index, value) => verticalCounts(index) = value }

    val hSums = rdd2.map(r => {
      val value = r.getInt(index1)
      val res = horizontalBoundaries.zipWithIndex.find(v => value <= v._1)
      (res.get._2, 1)
    }).groupByKey().mapValues(t => t.sum).collect()
    hSums.foreach { case (index, value) => horizontalCounts(index) = value }

//    println(verticalBoundaries.toList)
//    println(verticalCounts.toList)
//    println(horizontalBoundaries.toList)
//    println(horizontalCounts.toList)

    val buckets = M_Bucket_I(bucketsize, reducers, op).zipWithIndex
    val sortedR = rdd1.map(r => r.getInt(index1)).sortBy(identity)
    val sortedS = rdd2.map(r => r.getInt(index1)).sortBy(identity)

    val rWithBuckets = sortedR.zipWithIndex().map { case (v,i) =>
        val b = buckets.find { case (region, _) => region.rowS >= i && region.rowF <= i }
        (b.get._2, v)
    }

    val sWithBuckets = sortedS.zipWithIndex().map { case (v,i) =>
      val b = buckets.find { case (region, _) => region.colS >= i && region.colF <= i }
      (b.get._2, v)
    }

    rWithBuckets.zipPartitions(sWithBuckets){ case (i1, i2) => local_thetajoin(i1, i2, op)}
  }

  private def M_Bucket_I(bucketSize: Int, reducer: Int, op:String): Array[Region] = {
    var row = 0L
    val res: ArrayBuffer[Region] = ArrayBuffer.empty
    while (row < numR) {
      val (regions, r) = coverSubMatrix(row, bucketsize, reducers, op)
      row = regions.head.rowF
      if (r < 0)
        throw new IllegalArgumentException("Not enough reducers !")

      res.appendAll(regions)
    }
    res.toArray
  }

  private def coverSubMatrix(row: Long, bucketSize: Int, reducer: Int, op:String): (Array[Region], Int) = {
    var maxScore = 0L
    var rUsed = 0
    var bestRegions: Array[Region] = null
    for (i <- 0 until bucketSize) {
      if(row + i < numR) {
        val (regions, totalCandidatesCells) = coverRows(row, row + i, bucketSize, op)
        val score = if (regions.length == 0) 0 else totalCandidatesCells / regions.length
        if (score >= maxScore) {
          maxScore = score
          rUsed = regions.length
          bestRegions = regions
        }
      }
    }

    (bestRegions, reducers - rUsed)
  }

  private def coverRows (rowF: Long, rowL: Long, bucketSize: Int, op:String): (Array[Region], Long) = {
    var totalCandidateCells = 0L
    var currentRegionCells = 0L
    var startingCol = 0L
    val regions: ArrayBuffer[(Long, Long)] = ArrayBuffer.empty
    val vertical = getVerticalBoundaryIndexes(rowF,rowL)
    for (c <- 0L until numS) {
      val b = getHorizontalBoundaryIndex(c)
      val numberCandidateCells = vertical.foldLeft(0)((acc, v) => {
        val valid = checkIfValidBoundaries(v, b, op)
        val candidates = if (valid) verticalCounts(v._1) else 0
        acc + candidates
      })
      if (numberCandidateCells > 0) {
        if (currentRegionCells + numberCandidateCells > bucketSize) {
          regions.append((startingCol, c))
          startingCol = c
          currentRegionCells = 0
        }
        currentRegionCells += numberCandidateCells
        totalCandidateCells += numberCandidateCells
      }
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
      case "=" => (Rl >= Sl && Rl <= Su) || Ru >= Sl
      case "<" => Rl < Sl || Rl < Su
      case ">" => Rl > Sl || Rl > Su
      case "<=" => Rl <= Sl || Rl <= Su
      case ">=" => Rl >= Sl || Rl >= Su
      case "<>" => Rl > Su || Ru < Sl
    }
  }

  def getHorizontalBoundaryIndex(col: Long): (Int, Int) = {
    val c = col + 1
    var count = horizontalCounts.head
    var index = 0
    while (count < c) {
      index += 1
      count += horizontalCounts(index)
    }

    (index - 1, index)
  }

  def getVerticalBoundaryIndex(row: Long): (Int, Int) = {
    val r = row + 1
    var count = verticalCounts.head
    var index = 0
    while (count < r) {
      index += 1
      count += verticalCounts(index)
    }

    (index - 1, index)
  }

  def getVerticalBoundaryIndexes(rowF: Long, rowL: Long): List[(Int, Int)] = {
    val rowFIdx = getVerticalBoundaryIndex(rowF)
    val rowLIdx = getVerticalBoundaryIndex(rowL)

    (rowFIdx._1 to rowLIdx._1).zip(rowFIdx._2 to rowLIdx._2).toList
  }

//  private def getValidVerticalBoundaries(rowS: Long, rowF: Long): Array[(Int, Int)] = {
//    val start = findMatchingIndexBoundary(rowS, isHorizontal = false)
//    val end = findMatchingIndexBoundary(rowF, isHorizontal = false)
//    Array.tabulate[(Int, Int)](end-start+1)(i => (i+start, i+start+1))
//  }
//
//  private def findMatchingIndexBoundary(row: Long, isHorizontal: Boolean): Int = {
//    var firstIndex = 0
//    var lineNumber = 0
//    while (lineNumber < row) {
//      if (isHorizontal)
//        lineNumber += horizontalCounts(firstIndex)
//      else
//        lineNumber += verticalCounts(firstIndex)
//      firstIndex += 1
//    }
//    firstIndex
//  }

  /*
   * this method takes as input two lists of values that belong to the same partition
   * and performs the theta join on them. Both datasets are lists of tuples (Int, Int)
   * where ._1 is the partition number and ._2 is the value. 
   * Of course you might change this function (both definition and body) if it does not 
   * fit your needs :)
   * */  
  def local_thetajoin(dat1:Iterator[(Int, Int)], dat2:Iterator[(Int, Int)], op:String) : Iterator[(Int, Int)] = {
    var res = List[(Int, Int)]()
    val dat2List = dat2.toList
        
    while(dat1.hasNext) {
      val row1 = dat1.next()      
      for(row2 <- dat2List) {
        if(checkCondition(row1._2, row2._2, op)) {
          res = res :+ (row1._2, row2._2)
        }        
      }      
    }    
    res.iterator
  }  
  
  def checkCondition(value1: Int, value2: Int, op:String): Boolean = {
    op match {
      case "=" => value1 == value2
      case "<" => value1 < value2
      case "<=" => value1 <= value2
      case ">" => value1 > value2
      case ">=" => value1 >= value2
    }
  }
}

