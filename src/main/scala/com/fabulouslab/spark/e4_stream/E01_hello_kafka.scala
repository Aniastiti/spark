package com.fabulouslab.spark.e04_stream

//Nom du groupe: Ania Stiti, Nihel Chaabi, Amalou Kamilia, Amrane younis

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.functions.from_json
import org.apache.spark.streaming.{StreamingContext, _}

object E01_hello_kafka {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Hello Kafka")
      .master("local[2]")
      .getOrCreate()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "hello-kafka-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("pageviews")

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //Afficher le contenue du stream sur la console
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { partition =>
        val offset = offsetRanges(TaskContext.get.partitionId)
        partition.foreach { record =>
          println(s"Offset: ${offset.fromOffset}, Key: ${record.key}, Value: ${record.value}")
        }
      }
    }

    // Display the key and value in the console (format: String)
    stream.map(record => (record.key, record.value)).print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
