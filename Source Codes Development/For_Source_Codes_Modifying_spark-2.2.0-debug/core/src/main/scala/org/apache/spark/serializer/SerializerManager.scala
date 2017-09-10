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

package org.apache.spark.serializer

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.KMSwallow.{KMScalaKit, KMSwitch}

import scala.reflect.ClassTag
import org.apache.spark.SparkConf
import org.apache.spark.io.CompressionCodec
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.storage._
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * Component which configures serialization, compression and encryption for various Spark
 * components, including automatic selection of which [[Serializer]] to use for shuffles.
 */
private[spark] class SerializerManager(
    defaultSerializer: Serializer,
    conf: SparkConf,
    encryptionKey: Option[Array[Byte]]) {

  def this(defaultSerializer: Serializer, conf: SparkConf) = this(defaultSerializer, conf, None)

  private[this] val kryoSerializer = new KryoSerializer(conf)

  private[this] val stringClassTag: ClassTag[String] = implicitly[ClassTag[String]]
  private[this] val primitiveAndPrimitiveArrayClassTags: Set[ClassTag[_]] = {
    val primitiveClassTags = Set[ClassTag[_]](
      ClassTag.Boolean,
      ClassTag.Byte,
      ClassTag.Char,
      ClassTag.Double,
      ClassTag.Float,
      ClassTag.Int,
      ClassTag.Long,
      ClassTag.Null,
      ClassTag.Short
    )
    val arrayClassTags = primitiveClassTags.map(_.wrap)
    primitiveClassTags ++ arrayClassTags
  }

  // Whether to compress broadcast variables that are stored
  private[this] val compressBroadcast = conf.getBoolean("spark.broadcast.compress", true)
  
  // Whether to compress shuffle output that are stored
  private[this] val compressShuffle = conf.getBoolean("spark.shuffle.compress", true) //false //KMSwitch.compressShuffle //
  println(s"compressShuffle => [spark.shuffle.compress : ${this.compressShuffle}]")

  // Whether to compress RDD partitions that are stored serialized
  private[this] val compressRdds = conf.getBoolean("spark.rdd.compress", false)
  // Whether to compress shuffle output temporarily spilled to disk
  private[this] val compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true)

  /* The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet. */
//  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)
  private val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)

  def encryptionEnabled: Boolean = encryptionKey.isDefined

  def canUseKryo(ct: ClassTag[_]): Boolean = {
    primitiveAndPrimitiveArrayClassTags.contains(ct) || ct == stringClassTag
  }

  // SPARK-18617: As feature in SPARK-13990 can not be applied to Spark Streaming now. The worst
  // result is streaming job based on `Receiver` mode can not run on Spark 2.x properly. It may be
  // a rational choice to close `kryo auto pick` feature for streaming in the first step.
  def getSerializer(ct: ClassTag[_], autoPick: Boolean): Serializer = {
    if (autoPick && canUseKryo(ct)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  /**
   * Pick the best serializer for shuffling an RDD of key-value pairs.
   */
  def getSerializer(keyClassTag: ClassTag[_], valueClassTag: ClassTag[_]): Serializer = {
    if (canUseKryo(keyClassTag) && canUseKryo(valueClassTag)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  private def shouldCompress(blockId: BlockId): Boolean = {
    //return true
    blockId match {
      case _: ShuffleBlockId => compressShuffle
      case _: BroadcastBlockId => compressBroadcast
      case _: RDDBlockId => compressRdds
      case _: TempLocalBlockId => compressShuffleSpill
      case _: TempShuffleBlockId => compressShuffle
      case _ => false
    }
  }

  private def shouldSmartCompress(blockId: BlockId): Boolean = {
    if (blockId.isShuffle) {
//      if (!blockId.name.isEmpty()) {
//
//      }
      val flag: Boolean  = conf.getBoolean("spark.smartCompress", true)
      KMScalaKit.KMLogInfo(s"Get into method: [shouldSmartCompress]\n" +
        s"flag: ${flag}\n")
      return flag
    }
    else {
      val flag: Boolean = shouldCompress(blockId)
      return flag
    }
  }

  /**
   * Wrap an input stream for encryption and compression
   */
  def wrapStream(blockId: BlockId, s: InputStream): InputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
   * Wrap an output stream for encryption and compression
   */
  def wrapStream(blockId: BlockId, s: OutputStream): OutputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
   * Wrap an input stream for encryption if shuffle encryption is enabled
   */
  def wrapForEncryption(s: InputStream): InputStream = {
    encryptionKey
      .map { key => CryptoStreamUtils.createCryptoInputStream(s, conf, key) }
      .getOrElse(s)
  }

  /**
   * Wrap an output stream for encryption if shuffle encryption is enabled
   */
  def wrapForEncryption(s: OutputStream): OutputStream = {
    encryptionKey
      .map { key => CryptoStreamUtils.createCryptoOutputStream(s, conf, key) }
      .getOrElse(s)
  }

  /**
   * Wrap an output stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    KMScalaKit.KMLogInfo(s"Get into methods: [wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream]")
    //if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s

    if (shouldSmartCompress(blockId)) {
      println("OutputStream Compression is true")
      KMScalaKit.KMLogInfo(s"Compression is true\n" +
        s"compressShuffle     : ${this.compressShuffle}\n" +
        s"compressBroadcast   : ${this.compressBroadcast}\n" +
        s"compressRdds        : ${this.compressRdds}\n" +
        s"compressShuffleSpill: ${this.compressShuffleSpill}\n")
      return compressionCodec.compressedOutputStream(s)
    }
    else {
      println("OutputStream Compression is false")
      KMScalaKit.KMLogInfo(s"Compression is false\n" +
        s"compressShuffle     : ${this.compressShuffle}\n" +
        s"compressBroadcast   : ${this.compressBroadcast}\n" +
        s"compressRdds        : ${this.compressRdds}\n" +
        s"compressShuffleSpill: ${this.compressShuffleSpill}\n")
      return s
    }
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    KMScalaKit.KMLogInfo(s"Get into methods: [wrapForCompression(blockId: BlockId, s: InputStream): InputStream]")
    //if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s

    if (shouldSmartCompress(blockId)) {
      println("InputStream Compression is true")
      KMScalaKit.KMLogInfo(s"Compression is true\n" +
        s"compressShuffle     : ${this.compressShuffle}\n" +
        s"compressBroadcast   : ${this.compressBroadcast}\n" +
        s"compressRdds        : ${this.compressRdds}\n" +
        s"compressShuffleSpill: ${this.compressShuffleSpill}\n")
      return compressionCodec.compressedInputStream(s)
    }
    else {
      println("InputStream Compression is false")
      KMScalaKit.KMLogInfo(s"Compression is false\n" +
        s"compressShuffle     : ${this.compressShuffle}\n" +
        s"compressBroadcast   : ${this.compressBroadcast}\n" +
        s"compressRdds        : ${this.compressRdds}\n" +
        s"compressShuffleSpill: ${this.compressShuffleSpill}\n")
      return s
    }
  }

  /** Serializes into a stream. */
  def dataSerializeStream[T: ClassTag](
      blockId: BlockId,
      outputStream: OutputStream,
      values: Iterator[T]): Unit = {
    val byteStream = new BufferedOutputStream(outputStream)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = getSerializer(implicitly[ClassTag[T]], autoPick).newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
  }

  /** Serializes into a chunked byte buffer. */
  def dataSerialize[T: ClassTag](
      blockId: BlockId,
      values: Iterator[T]): ChunkedByteBuffer = {
    dataSerializeWithExplicitClassTag(blockId, values, implicitly[ClassTag[T]])
  }

  /** Serializes into a chunked byte buffer. */
  def dataSerializeWithExplicitClassTag(
      blockId: BlockId,
      values: Iterator[_],
      classTag: ClassTag[_]): ChunkedByteBuffer = {
    val bbos = new ChunkedByteBufferOutputStream(1024 * 1024 * 4, ByteBuffer.allocate)
    val byteStream = new BufferedOutputStream(bbos)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = getSerializer(classTag, autoPick).newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
    bbos.toChunkedByteBuffer
  }

  /**
   * Deserializes an InputStream into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserializeStream[T](
      blockId: BlockId,
      inputStream: InputStream)
      (classTag: ClassTag[T]): Iterator[T] = {
    val stream = new BufferedInputStream(inputStream)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    getSerializer(classTag, autoPick)
      .newInstance()
      .deserializeStream(wrapForCompression(blockId, inputStream))
      .asIterator.asInstanceOf[Iterator[T]]
  }
}
