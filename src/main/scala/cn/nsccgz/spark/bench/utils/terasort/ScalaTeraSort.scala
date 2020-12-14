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

package cn.nsccgz.spark.bench.utils.terasort

import java.util.Comparator
import com.google.common.primitives.UnsignedBytes
import org.apache.spark.{SparkConf, SparkContext}
import cn.nsccgz.spark.bench.utils.terasort.{TeraInputFormat, TeraOutputFormat, TeraSortPartitioner}
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * This is a great example program to stress test Spark's shuffle mechanism.
 *
 * See http://sortbenchmark.org/
 */
object ScalaTeraSort {

  implicit val caseInsensitiveOrdering : Comparator[Array[Byte]] =
    UnsignedBytes.lexicographicalComparator

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage:")
      println(
        "[input-file] [output-file]")
      println(" ")
      println("Example:")
      println(
        "/home/myuser/terasort_in /home/myuser/terasort_out")
      System.exit(0)
    }

    // Process command line arguments
    val inputFile = args(0)
    val outputFile = args(1)

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName(s"ScalaTeraSort")
    val sc = new SparkContext(conf)
    
    val startTime: Long =System.currentTimeMillis()
    
    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile)
    
    val endTime: Long=System.currentTimeMillis()
		System.out.println("reader time:" + (endTime - startTime) + "ms");
    
    val sorted = dataset.repartitionAndSortWithinPartitions(
      new TeraSortPartitioner(dataset.partitions.length))
      
    sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)
    
    sc.stop()
  }
}
