package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc : SparkContext) extends Serializable {

  var state = null
  var rdd_ratings: RDD[(Int, Int, Option[Double], Double, Int)] = null
  var rdd_titles: RDD[(Int, String, List[String])] = null
  var rdd_joined: RDD[(Int, String, List[String], Double)] = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings : RDD[(Int, Int, Option[Double], Double, Int)],
            title : RDD[(Int, String, List[String])]
          ) : Unit = {
    rdd_ratings = ratings
    rdd_titles = title

    val z = rdd_ratings.map({case(a,b,c,d,e)=>((a,b),(c,d,e))})
      .reduceByKey({case((y1,z1,l1) ,(y,z,l)) => if (l1 > l) (y1,z1,l1) else (y,z,l)})
      .map(x=>(x._1._1,x._1._2,x._2._1,x._2._2,x._2._3))

    val t1 = rdd_titles.map(x => (x._1, (x._2,x._3)))
    val t2 = z.map(x => (x._2, x._4))

    rdd_joined = t1.leftOuterJoin(t2).cache()
      .map({case(id,((title,keywords),rating)) => (id, title,keywords, rating)})
      .map({case(id,title,keyword,rating) => (id,title,keyword, rating.getOrElse(0.0))})

  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult() : RDD[(String, Double)] =  {

    var result = rdd_joined.map(x=>((x._1,x._2),x._4))
      .mapValues(value => (value, 1)) // map entry with a count of 1
      .reduceByKey {
        case ((sumL, countL), (sumR, countR)) =>
          (sumL + sumR, countL + countR)
      }
      .mapValues {
        case (sum , count) => sum / count
      }
    result.map(x=>(x._1._2,x._2))
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords : List[String]) : Double = {
    val z = rdd_ratings.map({case(a,b,c,d,e)=>((a,b),(c,d,e))})
      .reduceByKey({case((y1,z1,l1) ,(y,z,l)) => if (l1 > l) (y1,z1,l1) else (y,z,l)})
      .map(x=>(x._1._1,x._1._2,x._2._1,x._2._2,x._2._3))

    val t1 = rdd_titles.map(x => (x._1, (x._2,x._3)))
    val t2 = z.map(x => (x._2, x._4))

    val rdd_inner_joined = t1.join(t2).cache()
      .map({case(id,((title,keywords),rating)) => ((id,keywords), rating)})

    if (rdd_inner_joined.collect().size == 0)
      return 0

    var result = rdd_inner_joined.mapValues(value => (value, 1)) // map entry with a count of 1
      .reduceByKey {
        case ((sumL, countL), (sumR, countR)) =>
          (sumL + sumR, countL + countR)
      }
      .mapValues {
        case (sum , count) => sum / count
      }
      .filter(x=>(keywords.forall(x._1._2.contains)))


    val rollup = result.map(x=>x._2).collect()
    if (rollup.size ==0)
      return -1
    else
      return rollup.sum/rollup.size
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]) : Unit = {
    val rdd_delta = sc.parallelize(delta_)
    rdd_ratings = rdd_ratings.union(rdd_delta)
      .map({case(a,b,c,d,e)=>((a,b),(c,d,e))})
      .reduceByKey({case((y1,z1,l1) ,(y,z,l)) => if (l1 > l) (y1,z1,l1) else (y,z,l)})
      .map(x=>(x._1._1,x._1._2,x._2._1,x._2._2,x._2._3))

    val t1 = rdd_titles.map(x => (x._1, (x._2,x._3)))
    val t2 = rdd_ratings.map(x => (x._2, x._4))

    rdd_joined = t1.leftOuterJoin(t2).cache()
      .map({case(id,((title,keywords),rating)) => (id, title,keywords, rating)})
      .map({case(id,title,keyword,rating) => (id,title,keyword, rating.getOrElse(0.0))})

  }
}
