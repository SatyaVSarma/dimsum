/**
  * ELTDM ENSAE February 2017
  * Authors : A.ISNARDY, S. VENGATHESA-SARMA
  * Testing the DIMSUM implementation in Spark
  */

package DIMSUM_ENSAE


import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.SparseMatrix.sprand
import org.apache.spark.rdd.RDD

object Job {

  def main(args: Array[String]) : Unit = {

    implicit val sc = new SparkContext()

    // For illustration purposes, we generate a random sparse matrix with 100 000 lines and 1500 columns
    // Only 0.01% non zeros entries
    // Then we distribute it using Spark's RDDs
    // This step might take some time.
    // But obviously with real world examples one should have the sparse matrix ready to be loaded
    val rand = new java.util.Random
    val sparseMat = sprand(100000, 1500, 0.01, rand)
    def getRDD(m: SparseMatrix): RDD[Vector] = {
      val columns = m.toArray.grouped(m.numRows)
      val rows = columns.toSeq.transpose
      val vectors = rows.map(row => new DenseVector(row.toArray))
      sc.parallelize(vectors)
    }
    val rddFormat = getRDD(sparseMat)

    // Creating the sparse matrix from an RDD :
    val rowMatFormat = new RowMatrix(rddFormat)

    // Computing similarities by brute force without threshold
    // (ie naive method which computes dot products) :
    val startTime = System.currentTimeMillis
    val realSim = rowMatFormat.columnSimilarities()
    val elapsedTime = System.currentTimeMillis - startTime

    // With threshold (DIMSUM) : set a threshold under which similarities should not be considered
    val startTimeTh = System.currentTimeMillis
    val approxSim = rowMatFormat.columnSimilarities(0.01)
    val elapsedTimeTh = System.currentTimeMillis - startTimeTh

    // Evaluating the error of the approximation by computing the Mean Absolute Error :
    val realValues = realSim.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
    val approxValues = approxSim.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }
    val MAE = realValues.leftOuterJoin(approxValues).values.map {
      case (u, Some(v)) =>
        math.abs(u - v)
      case (u, None) =>
        math.abs(u)
    }.mean()

  }

}