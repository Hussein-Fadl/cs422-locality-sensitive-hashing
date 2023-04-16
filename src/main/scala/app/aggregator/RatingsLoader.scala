package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File
import scala.reflect.ClassTag.Any

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val ratings_path = new File(getClass.getResource(path).getFile).getPath

    val rdd_ratings = sc
      .textFile(ratings_path)
      .map(x => x.split('|'))
      .map(x => (x(0).toInt, x(1).toInt, None.asInstanceOf[Option[Double]], x(2).toDouble, x(3).toInt))

    rdd_ratings

  }
}
