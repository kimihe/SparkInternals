/**
  * Created by zhouqihua on 2017/8/11.
  */

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]): Unit = {
    val inputFile =  "./helloInput"
    val outputFile = "./helloOutput"



    println("************************ Step0: new SparkConf() begin ************************")
//    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val conf = new SparkConf().setAppName("KMSparkWordCount").setMaster("spark://zhouqihuadeMacBook-Pro.local:7077")
      .setJars(List("/Users/zhouqihua/Desktop/SparkWordCount-IDEA-sbt-Scala2_11_11/out/artifacts/SparkWordCount_IDEA_sbt_Scala2_11_11_jar/SparkWordCount-IDEA-sbt-Scala2_11_11.jar"))
        //.set("spark.executor.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005")
    println("************************ Step0: new SparkConf() end ************************")



    println("************************ Step1: new SparkContext(conf) begin ************************")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    println("************************ Step1: new SparkContext(conf) end ************************")



    println("************************ Step2: SparkContext.textFile(inputFile) begin ************************")
    // Load our input data.
    val input =  sc.textFile(inputFile)
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
    counts.saveAsTextFile(outputFile)
//    val collection = counts.collect()
//    println(s"Result: $collection")
    println("************************ Step6: saveAsTextFile(outputFile) end ************************")



    println("************************ Step7: Thread Sleep for 100s ... begin ************************")
    Thread.sleep(100000)
    println("************************ Step7: Thread Sleep for 100s ... end ************************")
  }
}

