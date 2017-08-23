/**
  * Created by zhouqihua on 2017/8/11.
  */

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]): Unit = {
    val inputFile =  "./helloInput"
    val outputFile = "./helloOutput"

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    // Create a Scala Spark Context.
    val sc = new SparkContext()
    // Load our input data.
    val input =  sc.textFile(inputFile)
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)
  }
}

