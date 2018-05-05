package streaming

import scala.util.hashing.MurmurHash3

/**
  * @author Morales Mikael <mikael.moralesgonzalez@epfl.ch>
  */
class CountMinSketch(delta: Double, eps: Double) {
  private val w: Int = math.ceil(math.E/eps).toInt
  private val d: Int = math.ceil(math.log(1/delta)).toInt

  private val matrix: Array[Array[Long]] = Array.ofDim(d,w)
  private val hashWithIndexes = (1 to d).map(hashFunction).zipWithIndex

  def update(key: (String, String), weight: Int = 1): Unit = {
    hashWithIndexes.foreach { case (hash, index) =>
        matrix(index)(hash(key)) += weight
    }
  }

  def get(key: (String, String)): Long = {
    hashWithIndexes.map {
      case (hash, index) => matrix(index)(hash(key))
    }.min
  }

  def hashFunction(seed: Int): ((String, String)) => Int = {
    case (x,y) => MurmurHash3.stringHash(x+y, seed) % w
  }
}