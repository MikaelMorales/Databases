package streaming

import scala.util.hashing.MurmurHash3

/**
  * Count-Min Sketch class, with d hash functions. It creates
  * a (d,w) matrix and each new key gets hashed d times and then it is added to
  * each row of the matrix with the given weight (default value is 1).
  * @author Morales Mikael <mikael.moralesgonzalez@epfl.ch>
  */
class CountMinSketch(delta: Double, eps: Double) {
  private val w: Int = math.ceil(math.E/eps).toInt
  private val d: Int = math.ceil(math.log(1/delta)).toInt

  private val matrix: Array[Array[Long]] = Array.ofDim(d,w)
  private val hashWithIndexes = (1 to d).map(hashFunction).zipWithIndex

  def update(key: (String, String), weight: Long = 1): Unit = {
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
    case (x,y) => Math.floorMod(MurmurHash3.stringHash(x+y, seed), w)
  }
}