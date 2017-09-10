/**
  * Created by zhouqihua on 2017/9/09.
  */


package org.apache.spark.KMSwallow

object KMSwitch {
  var compressShuffle: Boolean = true

  def description(): Unit = {
    println("[KMSwitch Description]:      \n" +
      s"compressShuffle  : ${this.compressShuffle} \n"
    )
  }
}
