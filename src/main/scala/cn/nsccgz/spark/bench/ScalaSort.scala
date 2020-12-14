/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.nsccgz.spark.bench

import cn.nsccgz.spark.bench.utils.IOCommon

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import java.util.concurrent.TimeUnit

object ScalaSort{
  implicit def rddToHashedRDDFunctions[K : Ordering : ClassTag, V: ClassTag]
         (rdd: RDD[(K, V)]) = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)
   
  val initTime = System.nanoTime()
  
  def main(args: Array[String]){
    if (args.length != 2){
      System.err.println(
        s"Usage: $ScalaSort <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaSort")
    val sc = new SparkContext(sparkConf)
    
    val startTime = System.nanoTime()

    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val reducer  = (parallel / 2)

    val io = new IOCommon(sc)
    val data = io.load[String](args(0)).map((_, 1))
    val partitioner = new HashPartitioner(partitions = reducer)
    val sorted = data.sortByKeyWithPartitioner(partitioner = partitioner).map(_._1)
    val size = sorted.count()
    System.out.println(s"Count: " + size)
//    io.save(args(1), sorted)
    
    val runningTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)
    System.out.println(s"ScalaSort runing time(seconds): " + runningTime)

    sc.stop()
    
    val totalTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - initTime)
    System.out.println(s"Total time(seconds): " + totalTime)
  }
}
