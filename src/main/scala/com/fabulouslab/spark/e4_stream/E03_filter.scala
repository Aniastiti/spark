package com.fabulouslab.spark.e04_stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, from_json}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object E03_filter {

  case class View(viewtime: Long, userid: String, pageid: String)

  def main(args: Array[String]) {

    /**
      *   Toujours sur le stream du topic pageviews :
      *   - Construire un stream qui contient seulement les événements sur la page 3
      *   - Construire un stream qui contient seulement les événements pour l'utilisateur 3
      *   - Construire un stream qui contient seulement les événements pour l'utilisateur 3 avec une durée de visite supérieure à 2 secondes
      *   - Réinjecter ces streams dans un topic Kafka
      * */

    // Créer une session Spark
    val spark = SparkSession.builder()
      .appName("Filtering Events")
      .master("local[2]")
      .getOrCreate()

    // Définir le schéma pour la structure JSON
    val jsonSchema = StructType(Seq(
      StructField("viewtime", LongType, nullable = true),
      StructField("userid", StringType, nullable = true),
      StructField("pageid", StringType, nullable = true)
    ))

    // Lire le flux Kafka en tant que DataFrame de strings
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "pageviews")
      .load()

    // Convertir la colonne "value" du DataFrame en JSON
    val jsonDF = kafkaDF.selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", jsonSchema).as("data"))
      .select("data.*")
    
    // Filtrer le flux pour les événements sur la page 3
    val page3Stream = jsonDF.filter($"pageid" === "page3")
    
    // Filtrer le flux pour les événements de l'utilisateur 3
    val user3Stream = jsonDF.filter($"userid" === "user3")

    // Filtrer le flux pour les événements de l'utilisateur 3 avec une durée de visite supérieure à 2 secondes
    val user3LongVisitStream = jsonDF.filter($"userid" === "user3" && $"viewtime" > 2000)

    // Réinjecter ces flux dans un topic Kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092"
    )

    page3Stream.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "page3_events")
      .start()

    user3Stream.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "user3_events")
      .start()

    user3LongVisitStream.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "user3_long_visit_events")
      .start()

    spark.streams.awaitAnyTermination()
  }

}
