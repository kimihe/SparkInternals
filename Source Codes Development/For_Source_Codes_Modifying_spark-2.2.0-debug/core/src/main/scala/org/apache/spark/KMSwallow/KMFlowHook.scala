/**
  * Created by zhouqihua on 2017/8/27.
  */

package org.apache.spark.KMSwallow

import scala.collection.mutable.ArrayBuffer

object KMFlowHook {
  val flows: ArrayBuffer[KMFlow] = ArrayBuffer[KMFlow]()
  val infos: ArrayBuffer[String] = ArrayBuffer[String]()

  def addOneInfo(aInfo: String): Unit = {
    this.infos += aInfo
  }

  def showInfo(): Unit = {
    val len = infos.length
    for (i <- 0 to (len-1)) {
      val aInfo = infos(i)
      println(s"infos($i): $aInfo")
    }
  }
}


class KMFlowHook() {



}
