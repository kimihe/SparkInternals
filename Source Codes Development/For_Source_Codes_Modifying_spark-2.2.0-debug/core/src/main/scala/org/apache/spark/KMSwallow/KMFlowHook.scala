/**
  * Created by zhouqihua on 2017/8/27.
  */

package org.apache.spark.KMSwallow

import scala.collection.mutable.ArrayBuffer

class KMFlowHook() {
  val flows: ArrayBuffer[KMFlow] = ArrayBuffer[KMFlow]();
}
