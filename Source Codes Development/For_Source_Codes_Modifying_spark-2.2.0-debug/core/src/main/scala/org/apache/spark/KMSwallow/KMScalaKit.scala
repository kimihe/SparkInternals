/**
  * Created by zhouqihua on 2017/8/27.
  */

package org.apache.spark.KMSwallow

import java.math.BigDecimal

import org.apache.spark.internal.Logging

object KMScalaKit extends Logging{
  def KMLogInfo(msg: String): Unit = {
    logInfo(s"\n" +
      s"******************** [KMLogInfo] ********************\n" +
      s"$msg\n")
  }

  def KMLogDebug(msg: String): Unit = {
    logDebug(s"\n" +
      s"******************** [KMLogDebug] ********************\n" +
      s"$msg\n")
  }

  def KMLogTrace(msg: String): Unit = {
    logTrace(s"\n" +
      s"******************** [KMLogTrace] ********************\n" +
      s"$msg\n")
  }

  def KMLogWarning(msg: String): Unit = {
    logWarning(s"\n" +
      s"******************** [KMLogWarning] ********************\n" +
      s"$msg\n")
  }

  def KMLogError(msg: String): Unit = {
    logError(s"\n" +
      s"******************** [KMLogError] ********************\n" +
      s"$msg\n")
  }

  def bigDemicalDoubleAdd(number1: Double, number2: Double): Double = {
    val a: BigDecimal = new BigDecimal(number1.toString);
    val b: BigDecimal = new BigDecimal(number2.toString);
    val sum: Double =  a.add(b).doubleValue();

    return  sum;
  }

  def bigDemicalDoubleAdd(number1: Double, number2: Double, number3: Double): Double = {
    val a: BigDecimal = new BigDecimal(number1.toString);
    val b: BigDecimal = new BigDecimal(number2.toString);
    val c: BigDecimal = new BigDecimal(number3.toString);
    val sum: Double =  a.add(b).add(c).doubleValue();

    return  sum;
  }

  def bigDemicalDoubleSub(number1: Double, number2: Double): Double = {
    val a: BigDecimal = new BigDecimal(number1.toString);
    val b: BigDecimal = new BigDecimal(number2.toString);
    val res: Double =  a.subtract(b).doubleValue();

    return  res;
  }

  def bigDemicalDoubleMul(number1: Double, number2: Double): Double = {
    val a: BigDecimal = new BigDecimal(number1.toString);
    val b: BigDecimal = new BigDecimal(number2.toString);
    val res: Double = a.multiply(b).doubleValue();

    return res;
  }

  def bigDemicalDoubleMul(number1: Double, number2: Double, number3: Double): Double = {
    val a: BigDecimal = new BigDecimal(number1.toString);
    val b: BigDecimal = new BigDecimal(number2.toString);
    val c: BigDecimal = new BigDecimal(number2.toString);
    val res: Double = a.multiply(b).multiply(c).doubleValue();

    return res;
  }

  def bigDemicalDoubleDiv(number1: Double, number2: Double): Double = {
    val a: BigDecimal = new BigDecimal(number1.toString);
    val b: BigDecimal = new BigDecimal(number2.toString);
    val res: Double =  a.divide(b).doubleValue();

    return  res;
  }
}
