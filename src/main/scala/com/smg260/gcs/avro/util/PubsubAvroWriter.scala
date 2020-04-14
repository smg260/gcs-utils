package com.smg260.gcs.avro.util

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.bouncex.platformx.common.avro.UnsubscribedResult
import com.google.api.core.{ApiFuture, ApiFutureCallback, ApiFutures}
import com.google.api.gax.batching.BatchingSettings
import com.google.cloud.pubsub.v1.Publisher
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.smg260.avro.util.AvroEncoder
import org.rogach.scallop.ScallopConf
import org.threeten.bp.Duration

import scala.collection.JavaConverters._

/**
 * java -jar target/pubber.jar --topic projects/bx-dev-platformx/topics/poc-joiner-input
 * send
 * -b "false"
 * -n 100
 * --batchSize 10000
 * -Awebsite_id=1828 batch_id=1 result_key=1238:d@email.com
 *
 * @param arg
 */
class PubsubAvroConf(arg: Seq[String]) extends ScallopConf(arg) {
  val topic = opt[String](default = Some("projects/bx-dev-platformx/topics/mg-unsub-out"))
  val batchSize = opt[Long](name = "batchSize", short = 's', default = Some(1000))

  //e.g. -A result_key=elig#{id}
  val attribs = props[String]('A', descr = "attrib value can contain {id} to insert index of message")
  val num = opt[Int](name = "num", default = Some(1))
  verify()
}

object PubsubAvroWriter {
  val success = new AtomicLong()
  val failed = new AtomicLong()

  def main(args: Array[String]): Unit = {
    def options = new PubsubAvroConf(args)

    var publisher: Publisher = null
    try {

      val batchSettings = BatchingSettings.newBuilder()
        .setIsEnabled(true)
        .setElementCountThreshold(options.batchSize())
        .setDelayThreshold(Duration.ofMillis(500)).build()

      publisher = Publisher.newBuilder(options.topic()).setBatchingSettings(batchSettings).build()

      val encoder = new AvroEncoder[UnsubscribedResult](UnsubscribedResult.getClassSchema)
      val generated = encoder.generateRandom(10)

      generated.asScala
        .zipWithIndex
        .foreach{ case (r, i) =>
          val attribs = options.attribs.mapValues(_.replaceAllLiterally("{id}", i.toString))
          publishMessage(publisher, encoder.encode(r), attribs)
//          println(attribs.mkString(","))
        }

    } finally {

      if (publisher != null) {
        publisher.shutdown()
        publisher.awaitTermination(1, TimeUnit.MINUTES)
      }

      println(s"Succeeded:\t $success")
      println(s"Errored:\t $failed")
    }
  }

  def publishMessage(publisher: Publisher, data: ByteString, attributes: Map[String, String]): Unit = {
    val message = PubsubMessage.newBuilder()
      .setData(data)
      .putAllAttributes(attributes.asJava)
      .putAttributes("publish_timestamp", System.currentTimeMillis() + "").build()
    val async: ApiFuture[String] = publisher.publish(message)

    ApiFutures.addCallback(async, new ApiFutureCallback[String] {
      override def onFailure(throwable: Throwable): Unit = failed.incrementAndGet()

      override def onSuccess(v: String): Unit = success.incrementAndGet()
    }, MoreExecutors.directExecutor())
  }
}
