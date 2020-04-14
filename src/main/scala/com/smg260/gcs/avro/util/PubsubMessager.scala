package com.smg260.gcs.avro.util

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.google.api.core.{ApiFuture, ApiFutureCallback, ApiFutures}
import com.google.api.gax.batching.BatchingSettings
import com.google.cloud.pubsub.v1.Publisher
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.rogach.scallop.{ScallopConf, Subcommand}
import org.threeten.bp.Duration

import scala.collection.JavaConversions._
import scala.io.StdIn
import scala.util.{Success, Try}

/**
 * java -jar target/pubber.jar --topic projects/bx-dev-platformx/topics/poc-joiner-input
 * send
 * -b "false"
 * -n 100
 * --batchSize 10000
 * -Awebsite_id=1828 batch_id=1 result_key=1238:d@email.com
 * @param arg
 */
class Conf(arg: Seq[String]) extends ScallopConf(arg) {
  val topic = opt[String](default = Some("projects/bx-dev-platformx/topics/mg_test"))
  val send = new Subcommand("send") {
    val body = opt[String](required = true)
    val batchSize = opt[Long](name = "batchSize", short = 's', default = Some(1000))
    val attribs = props[String]('A')
    val num = opt[Int](default = Some(1))
  }

  addSubcommand(send)
  verify()
}

object PubsubMessager {
  val success = new AtomicLong()
  val failed = new AtomicLong()

  def main(args: Array[String]): Unit = {
    def options = new Conf(args)

    var publisher: Publisher = null
    try {

      if (options.subcommands.nonEmpty) {
        val batchSettings = BatchingSettings.newBuilder()
            .setIsEnabled(true)
            .setElementCountThreshold(options.send.batchSize())
            .setDelayThreshold(Duration.ofMillis(500)).build()

        publisher = Publisher.newBuilder(options.topic()).setBatchingSettings(batchSettings).build()
        (0 until options.send.num()).foreach(_ =>
          publishMessage(publisher, options.send.body(), options.send.attribs))
      } else {
        publisher = Publisher.newBuilder(options.topic()).build()
        while (true) {
          val s = StdIn.readLine()
          val parts = s.split("\\s")

          val (numMessages, data) = Try(parts.last.toInt) match {
            case (Success(num)) => (num, parts.dropRight(1).mkString(" "))
            case _ => (1, s)
          }

          (0 until numMessages).foreach(_ => publishMessage(publisher, data, Map.empty))
        }
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

  def publishMessage(publisher: Publisher, data: String, attributes: Map[String, String]): Unit = {
    val message = PubsubMessage.newBuilder()
      .setData(ByteString.copyFromUtf8(data))
      .putAllAttributes(attributes)
      .putAttributes("publish_timestamp", System.currentTimeMillis() + "").build()
    val async: ApiFuture[String] = publisher.publish(message)

    ApiFutures.addCallback(async, new ApiFutureCallback[String] {
      override def onFailure(throwable: Throwable): Unit = failed.incrementAndGet()

      override def onSuccess(v: String): Unit = success.incrementAndGet()
    }, MoreExecutors.directExecutor())
  }
}
