/**
  * Created by zhouqihua on 2017/8/23.
  */

import org.apache.spark.{SparkContext, SparkConf}
import java.io.File

object SparkWordCount {
  def main(args: Array[String]): Unit = {

    //    val log: Logger = Logger.
    val INPUT_HDFS =  "./INPUT_HDFS"
    val OUTPUT_HDFS = "./OUTPUT_HDFS_WORDCOUNT"

    val outputPath: File = new File(OUTPUT_HDFS)
    if (outputPath.exists())
      deleteDir(outputPath)




      println("************************ Step0: new SparkConf() begin ************************")
    //    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val conf = new SparkConf().setAppName("SparkWordCount")
      .setMaster("spark://zhouqihuadeMacBook-Pro.local:7077")
      .setJars(List("/Users/zhouqihua/Desktop/For_Source_Codes_Reading_SparkWordCount-IDEA-sbt-Scala2_11_11/out/artifacts/SparkWordCount/For_Source_Codes_Reading_SparkWordCount-IDEA-sbt-Scala2_11_11.jar"))
      .set("spark.shuffle.compress", "false")
      .set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec")
    //.set("spark.executor.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005")
    println("************************ Step0: new SparkConf() end ************************")



    println("************************ Step1: new SparkContext(conf) begin ************************")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    println("************************ Step1: new SparkContext(conf) end ************************")



    println("************************ Step2: SparkContext.textFile(inputFile) begin ************************")
    // Load our input data.
    val input =  sc.textFile(INPUT_HDFS)
    println("************************ Step2: SparkContext.textFile(inputFile) end ************************")



    println("************************ Step3: flatMap begin ************************")
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    println("************************ Step3: flatMap end ************************")



    println("************************ Step4: map begin ************************")
    // Transform into word and count.
    val mapWords = words.map(word => (word, 1))
    println("************************ Step4: map end ************************")



    println("************************ Step5: reduceByKey begin ************************")
    val counts = mapWords.reduceByKey{case (x, y) => x + y}
    println("************************ Step5: reduceByKey end ************************")



    println("************************ Step6: saveAsTextFile(outputFile) begin ************************")
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(OUTPUT_HDFS)
    //    val collection = counts.collect()
    //    println(s"Result: $collection")
    println("************************ Step6: saveAsTextFile(outputFile) end ************************")



    println("************************ Step7: Thread Sleep for 100s ... begin ************************")
    Thread.sleep(100000)
    println("************************ Step7: Thread Sleep for 100s ... end ************************")
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