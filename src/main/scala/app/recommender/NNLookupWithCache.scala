package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions

import scala.collection.mutable.HashMap
import scala.math.Ordering.Implicits.seqDerivedOrdering

/**
 * Class for performing LSH lookups (enhanced with cache)
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookupWithCache(lshIndex: LSHIndex) extends Serializable {
  var cache: RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = null
  var countByKey: Broadcast[collection.Map[IndexedSeq[Int], Long]] =
    SparkContext.getOrCreate().broadcast(lshIndex.getBuckets().countByKey())

  var histogram :  RDD[((IndexedSeq[Int], List[String]), Long)] = null
    //lshIndex.getBuckets().mapValues((x => x.length.toLong))
  //    sqlContext.sparkContext.broadcast(buckets.countByKey())

  /**
   *
   * The operation for buiolding the cache
   *
   * @param sc Spark context for current application
   */
  def build(sc: SparkContext): Unit = {

    var rdd: RDD[(IndexedSeq[Int],List[String])] = null
    var total = histogram.map(x=>x._2).reduce((x,y)=>x+y)
    var histogram_norm = histogram.map(x=>(x._1,x._2.toFloat/total.toFloat)).filter(x=>(x._2>0.01)).map(x=>x._1)
    val buckets = lshIndex.getBuckets()
    cache = buckets.join(histogram_norm).map(x=>(x._1,x._2._1))

  }

  /**
   * Testing operation: force a cache based on the given object
   *
   * @param ext A broadcast map that contains the objects to cache
   */
  def buildExternal(ext: Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]]): Unit = {
    var rddMap = ext.value
    cache = SparkContext.getOrCreate().parallelize(rddMap.toList)
  }

  /**
   * Lookup operation on cache
   *
   * @param queries The RDD of keyword lists
   * @return The pair of two RDDs
   *         The first RDD corresponds to queries that result in cache hits and
   *         includes the LSH results
   *         The second RDD corresponds to queries that result in cache hits and
   *         need to be directed to LSH
   */
  def cacheLookup(queries: RDD[List[String]])
  : (RDD[(List[String], List[(Int, String, List[String])])], RDD[(IndexedSeq[Int], List[String])]) = {

    var hits: RDD[(List[String], List[(Int, String, List[String])])] = null
    var misses: RDD[(IndexedSeq[Int], List[String])] = null

    val hashed: RDD[(IndexedSeq[Int], List[String])] = lshIndex.hash(queries)

    /* Computing the histogram */
    val hash_count = hashed.map(x => (x,1L))

    val current_histogram = hash_count.reduceByKey((x,y) => x+y)

    if (histogram == null) {
      histogram = current_histogram
    } else {
      histogram = histogram.fullOuterJoin(current_histogram).map(x=>(x._1,x._2._1.getOrElse(0L)+x._2._2.getOrElse(0L)))
    }

//    val s = hashed
//          .map { case (a, _) => (a, counts.getOrElse(a, 0L).toInt+1) }
//          .distinct()
//          .sortByKey()

    if (cache == null) {
      hits = null
      misses = hashed
    } else {
      val joined = hashed.join(cache)
      hits = joined.map(x => (x._2._1, x._2._2))
      misses = hashed.leftOuterJoin(cache).map(x => (x._1, x._2._1)).subtract(joined.map(x => (x._1, x._2._1)))
    }

    return (hits, misses)
  }

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, result) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    var cresult = cacheLookup(queries)
    var hits: RDD[(List[String], List[(Int, String, List[String])])] = cresult._1
    var misses: RDD[(IndexedSeq[Int], List[String])] = cresult._2

    val misses_result = lshIndex.lookup(misses).map(x => (x._2, x._3))

    if (hits == null) {
      misses_result
    } else {
      hits.union(misses_result)
    }
  }
}
