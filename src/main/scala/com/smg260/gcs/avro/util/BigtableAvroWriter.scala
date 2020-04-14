package com.smg260.gcs.avro.util

import com.bouncex.platformx.eligibility.avro.EligibilityResult
import com.google.cloud.bigtable.data.v2.models.{BulkMutation, Query, RowMutationEntry}
import com.google.cloud.bigtable.data.v2.{BigtableDataClientFactory, BigtableDataSettings}
import com.google.protobuf.ByteString
import com.smg260.avro.util.AvroEncoder
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.collection.JavaConverters._

object BigtableAvroWriter extends App {

  class Conf(arg: Seq[String]) extends ScallopConf(arg) {
    val project = opt[String](name = "project", default = Some("bx-dev-platformx"))
    val instance = opt[String](name = "instance", default = Some("ingestion"))
    val table = opt[String](name = "table", default = Some("eligibility_result"))
    val keyPrefix = opt[String](name = "keyPrefix", default = Some("elig#"))

    val create = new Subcommand("create") {
      //keys will be written with keyPrefix(startFrom + index)
      val num = opt[Int](name = "num", default = Some(10))
      val startFrom = opt[Int](name = "startFrom", default = Some(0))
    }

    val delete = new Subcommand("delete") {
      val startIndex = opt[Int](name = "startIndex")
      val endIndex = opt[Int](name = "endIndex")
    }

    val list = new Subcommand("list") {

    }

    addSubcommand(create)
    addSubcommand(delete)
    addSubcommand(list)
    verify()
  }

  val conf = new Conf(args)

  val client = BigtableDataClientFactory.create(
    BigtableDataSettings.newBuilder()
      .setInstanceId(conf.instance())
      .setProjectId(conf.project()).build()).createDefault()

  if(conf.subcommand.contains(conf.list)) {
    println("reading")
    client.readRows(Query.create(conf.table()).prefix(conf.keyPrefix()))
      .iterator().asScala
      .foreach(r => println(r.getKey.toStringUtf8))
  } else {
    val bulkMutation = if (conf.subcommand.contains(conf.create)) {
      println("creating IDs")
      val encoder = new AvroEncoder[EligibilityResult](EligibilityResult.getClassSchema)
      val generated = encoder.generateRandom(conf.create.num())

      generated.asScala
        .zipWithIndex
        .map { case (r, i) =>
          val id = s"${conf.keyPrefix()}${i + conf.create.startFrom()}"
          println(id)
          RowMutationEntry.create(id)
            .setCell("cf", ByteString.copyFromUtf8("a"), encoder.encode(r))
        }
        .foldLeft(BulkMutation.create(conf.table())) { (bm, rm) => bm.add(rm) }
      //  } else if(conf.subcommand.contains(conf.delete)) {
    } else {
      println("deleting IDs")
      BulkMutation.create(conf.table())

      (conf.delete.startIndex() to conf.delete.endIndex())
        .map { i =>
          val id = s"${conf.keyPrefix()}$i"
          println(id)
          RowMutationEntry.create(id).deleteRow()
        }
        .foldLeft(BulkMutation.create(conf.table())) { (bm, rm) => bm.add(rm) }
    }

    client.bulkMutateRows(bulkMutation)
  }

  client.close()
}
