package app.recommender

import app.Main.getClass
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class TitlesLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val rdd_path = new File(getClass.getResource(path).getFile).getPath

    val rdd_corpus = sc
      .textFile(rdd_path)
      .map(x => x.split('|'))
      .map(x => (x(0).toInt, x(1), x.slice(2, x.length).toList))

    rdd_corpus
  }
}
