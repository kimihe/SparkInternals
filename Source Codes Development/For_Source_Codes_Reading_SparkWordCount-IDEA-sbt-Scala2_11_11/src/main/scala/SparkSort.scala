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

//package com.intel.hibench.sparkbench.micro

import java.io.File

import SparkWordCount.deleteDir
import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.spark.ConfigurableOrderedRDDFunctions
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object SparkSort{
  implicit def rddToHashedRDDFunctions[K : Ordering : ClassTag, V: ClassTag]
  (rdd: RDD[(K, V)]) = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  def main(args: Array[String]){

    val INPUT_HDFS =  "./INPUT_HDFS"
    val OUTPUT_HDFS = "./OUTPUT_HDFS_SORT"

    val outputPath: File = new File(OUTPUT_HDFS)
    if (outputPath.exists())
      deleteDir(outputPath)


//    if (args.length != 2){
//      System.err.println(
//        s"Usage: $SparkSort <INPUT_HDFS> <OUTPUT_HDFS>"
//      )
//      System.exit(1)
//    }

    val sparkConf = new SparkConf().setAppName("ScalaSort")
                                   .setMaster("spark://zhouqihuadeMacBook-Pro.local:7077")
                                   .setJars(List("/Users/zhouqihua/Desktop/For_Source_Codes_Reading_SparkWordCount-IDEA-sbt-Scala2_11_11/out/artifacts/SparkSort/For_Source_Codes_Reading_SparkWordCount-IDEA-sbt-Scala2_11_11.jar"))


    val sc = new SparkContext(sparkConf)

    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
//    val reducer  = IOCommon.getProperty("hibench.default.shuffle.parallelism")
//      .getOrElse((parallel / 2).toString).toInt
    val reducer  = ((parallel / 2).toString).toInt

    val io = new IOCommon(sc)
    val data = io.load[String](INPUT_HDFS).map((_, 1))
    val partitioner = new HashPartitioner(partitions = reducer)
    val sorted = data.sortByKeyWithPartitioner(partitioner = partitioner).map(_._1)

    io.save(OUTPUT_HDFS, sorted)
    sc.stop()
  }

  def deleteDir(dir: File): Unit = {
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
        println(s"Delete File: ${f.getAbsolutePath}")
      }
    })
    dir.delete()
    println(s"Delete Dir:  ${dir.getAbsolutePath}\n" )
  }
}
