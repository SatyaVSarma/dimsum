/**
  * ELTDM ENSAE February 2017
  * A.ISNARDY, S. VENGATHESA-SARMA
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

    /*
    import com.jmatio.io.MatFileReader

    // Reading the Specular sparse matrix : 477976	rows, 1600	columns
    val file = new MatFileReader("Path_to_file_in_matlab_format")
    val content = file.getContent

    val matrix = content.get("x$1").asInstanceOf[Int]
    */

    // For illustration purposes, we generate a random sparse matrix with 500 000 lines and 1500 columns
    // Only 0.01% non zeros entries
    // Then we distribute it using Spark's RDDs
    val rand = new java.util.Random
    val sparseMat = sprand(500000, 1500, 0.01, rand)

    def getRDD(m: SparseMatrix): RDD[Vector] = {
      val columns = m.toArray.grouped(m.numRows)
      val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
      val vectors = rows.map(row => new DenseVector(row.toArray))
      sc.parallelize(vectors)
    }

    val rddFormat = getRDD(sparseMat)

    val rowMatFormat = new RowMatrix(rddFormat)

    // Brute force without threshold (naive method computing dot products) : 49.162 s for 50k and 10 m3
    val startTime = System.currentTimeMillis
    val exactSim = rowMatFormat.columnSimilarities()
    val elapsedTime = System.currentTimeMillis - startTime

    /** With threshold (DIMSUM) : set a threshold under which similarities are not considered
      * 6.002 s for 0.01 similarity threshold
      * 5.996 s for 0.2
      * 5.975 s for 0.5
      * 5.829 s for 0.8
      * */
    val startTimeTh = System.currentTimeMillis
    val approxSim = rowMatFormat.columnSimilarities(0.01)
    val elapsedTimeTh = System.currentTimeMillis - startTimeTh

    // Evaluating the error of the approximation :

    val exactEntries = exactSim.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
    val approxEntries = approxSim.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }
    val MAE = exactEntries.leftOuterJoin(approxEntries).values.map {
      case (u, Some(v)) =>
        math.abs(u - v)
      case (u, None) =>
        math.abs(u)
    }.mean()


    // MAE 0.8 :0.00314379804491441
    // MAE 0.5 : 0.0011766723906978475
    // MAE 0.2 : 8.27489670489529E-19
    // MAE 0.01 : 7.67089307165579E-19


  }

}
