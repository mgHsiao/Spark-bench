package cn.nsccgz.spark.bench

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import java.util.concurrent.TimeUnit

object ScalaPageRank {
    val initTime = System.nanoTime()
  
   def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ScalaPageRank <input_file> <output_filename> [<iter>]")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaPageRank").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val input_path = args(0)
    val output_path = args(1)
    val iters = if (args.length > 2) args(2).toInt else 10
    val ctx = new SparkContext(sparkConf)
    
    val startTime = System.nanoTime()

//  Modified by Lv: accept last two values from HiBench generated PageRank data format
    val lines = ctx.textFile(input_path, 1)
    val links = lines.map{ s =>
      val elements = s.split("\\s+")
      val parts = elements.slice(elements.length - 2, elements.length)
      (parts(0), parts(1))
    }.distinct().groupByKey() //.cache() // mem bug?
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

//    val output = ranks.collect()
//    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    
//    val io = new IOCommon(ctx)
//    io.save(output_path, ranks)
    
//    ranks.saveAsTextFile(output_path)
    val size:Long=ranks.count()
    System.out.println(s"Count: " + size)
    
    val runningTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)
    System.out.println(s"ScalaPageRank runing time(seconds): " + runningTime)

    ctx.stop()
    
    val totalTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - initTime)
    System.out.println(s"Total time(seconds): " + totalTime)
  }
  
}