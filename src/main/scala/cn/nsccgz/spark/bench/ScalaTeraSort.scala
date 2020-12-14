
package cn.nsccgz.spark.bench

import java.util.concurrent.TimeUnit
import org.apache.hadoop.examples.terasort.{TeraInputFormat, TeraOutputFormat}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.spark._
import org.apache.spark.rdd._

import scala.reflect.ClassTag

object ScalaTeraSort {
  implicit def rddToSampledOrderedRDDFunctions[K: Ordering : ClassTag, V: ClassTag]
  (rdd: RDD[(K, V)]) = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  implicit def ArrayByteOrdering: Ordering[Array[Byte]] = Ordering.fromLessThan {
    case (a, b) => (new BytesWritable(a).compareTo(new BytesWritable(b))) < 0
  }
  
  val initTime = System.nanoTime()

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        s"Usage: $ScalaTeraSort <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaTeraSort").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
//    val io = new IOCommon(sc)

    //val file = io.load[String](args(0), Some("Text"))
//    val startTime: Long =System.currentTimeMillis()
    val startTime = System.nanoTime()
     
    val data = sc.newAPIHadoopFile[Text, Text, TeraInputFormat](args(0)).map {
      case (k,v) => (k.copyBytes, v.copyBytes)
    }
    
//    val endTime: Long=System.currentTimeMillis()
//		System.out.println("reader time:" + (endTime - startTime) + "ms");
        
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
//    val reducer  = IOCommon.getProperty("hibench.default.shuffle.parallelism")
//      .getOrElse((parallel / 2).toString).toInt
//    val reducer:Int = Math.floor(parallel * 0.8).toInt
    val reducer: Int = (parallel / 2).toInt
    
//    System.err.println(s"parallel: " + parallel)

    val partitioner = new BaseRangePartitioner(partitions = reducer, rdd = data)
    val ordered_data = new ConfigurableOrderedRDDFunctions[Array[Byte], Array[Byte], (Array[Byte], Array[Byte])](data)
    val sorted_data = ordered_data.sortByKeyWithPartitioner(partitioner = partitioner).map{case (k, v)=>(new Text(k), new Text(v))}

//    sorted_data.saveAsNewAPIHadoopFile[TeraOutputFormat](args(1))
    //io.save(args(1), sorted_data)
    
    val size:Long =sorted_data.count();
    System.out.println(s"Count: " + size)
    
    val runningTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)
    System.out.println(s"ScalaTeraSort runing time(seconds): " + runningTime)

    sc.stop()
    
    val totalTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - initTime)
    System.out.println(s"Total time(seconds): " + totalTime)
  }
}
