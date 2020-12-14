
package cn.nsccgz.spark.bench

import org.apache.spark.{SparkConf, SparkContext}
import cn.nsccgz.spark.bench.utils.IOCommon
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.api.materializeTypeTag

import org.apache.hadoop.io.{NullWritable, Text}
import java.util.concurrent.TimeUnit

/*
 * Adopted from spark's example: https://spark.apache.org/examples.html
 */
object ScalaWordCount{
  val initTime = System.nanoTime()
  
  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaWordCount <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaWordCount")
    val sc = new SparkContext(sparkConf)
    
    val startTime = System.nanoTime()

    val io = new IOCommon(sc)
    val data = io.load[String](args(0))
    val counts = data.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _)
    
//    val sequence_data = counts.map(x => (NullWritable.get(), new Text(x.toString)))
//    val sequence_data = counts.map(x => new Text(x.toString))
//    val size:Long =sequence_data.count();
                     
    val size:Long =counts.count();
    System.out.println(s"Count: " + size)
    
//    io.save(args(1), counts, outputformat)
        
    val runningTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)
    System.out.println(s"ScalaWordCount runing time(seconds): " + runningTime)

    sc.stop()
    
    val totalTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - initTime)
    System.out.println(s"Total time(seconds): " + totalTime)
  }
}