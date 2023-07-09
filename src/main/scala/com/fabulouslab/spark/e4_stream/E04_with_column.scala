package com.fabulouslab.spark.e04_stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, from_json, col, expr}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object E04_with_column {

  def main(args: Array[String]) {

    /**
      * La fonction withColumn permet de créer un nouveau dataframe en ajoutant une nouvelle colonne,
      * elle est souvent associée à des fonctions dans le package org.apache.spark.sql.functions.
      * En utilisant withColumn:
      *   - Enrichir le stream en ajoutant 2 colonnes :
      *      - La date de traitement de l'event
      *      - Le temps passé sur la page en second
      * */

    // Créer une session Spark
    val spark = SparkSession.builder()
      .appName("Enriching Stream")
      .master("local[2]")
      .getOrCreate()

    // Définir le schéma pour la structure JSON
    val jsonSchema = StructType(Seq(
      StructField("viewtime", LongType, nullable = true),
      StructField("userid", StringType, nullable = true),
      StructField("pageid", StringType, nullable = true)
    ))

    // Lire le flux Kafka en tant que DataFrame de chaînes de caractères
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "pageviews")
      .load()

    // Convertir la colonne "value" du DataFrame en JSON
    val jsonDF = kafkaDF.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), jsonSchema).as("data"))
      .select("data.*")

    // Ajouter une colonne pour la date de traitement de l'événement
    val enrichedDF = jsonDF.withColumn("processing_date", current_date())

    // Ajouter une colonne pour le temps passé sur la page en secondes
    val enrichedDFWithTime = enrichedDF.withColumn("time_spent_seconds", expr("viewtime / 1000"))

    // Afficher le DataFrame résultant
    val query = enrichedDFWithTime.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

}
