/**
  * Created by zhouqihua on 2017/8/30.
  */

// scalastyle:off println

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Usage: BroadcastTest [partitions] [numElem] [blockSize]
  */
object SparkBroadcastTest {
  def main(args: Array[String]) {

    println("************************ Step0: new SparkConf() begin ************************")
    //    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("spark://zhouqihuadeMacBook-Pro.local:7077")
      .setJars(List("/Users/zhouqihua/Desktop/For_Source_Codes_Reading_SparkWordCount-IDEA-sbt-Scala2_11_11/out/artifacts/for_source_codes_reading_sparkwordcount_idea_sbt_scala2_11_11_jar/for_source_codes_reading_sparkwordcount-idea-sbt-scala2_11_11.jar"))
    //.set("spark.executor.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005")
    println("************************ Step0: new SparkConf() end ************************")


    val blockSize = if (args.length > 2) args(2) else "4096"

    val spark = SparkSession
      .builder()
      .appName("SparkBroadcastTest")
      .config("spark.broadcast.blockSize", blockSize)
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = (0 until num).toArray

    for (i <- 0 until 3) {
      println("Iteration " + i)
      println("===========")
      val startTime = System.nanoTime
      val barr1 = sc.broadcast(arr1)
      val observedSizes = sc.parallelize(1 to 10, slices).map(_ => barr1.value.length)
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    }

    spark.stop()
  }
}
// scalastyle:on println

