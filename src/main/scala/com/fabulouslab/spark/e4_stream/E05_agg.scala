package com.fabulouslab.spark.e04_stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, from_json}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object E05_agg {

  def main(args: Array[String]) {

    /**
      *   - Compter le temps moyen passé sur une page,
      *   - Compter le temps max passé sur une page,
      *   - Compter en une seule fois le temps moyen et max passé sur une page.
      * */

    // Créer une session Spark
    val spark = SparkSession.builder()
      .appName("Aggregating Stream")
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

    // Calculer le temps moyen passé sur une page
    val avgTimeDF = jsonDF.selectExpr("avg(viewtime) as avg_time_spent")

    // Calculer le temps max passé sur une page
    val maxTimeDF = jsonDF.selectExpr("max(viewtime) as max_time_spent")

    // Calculer le temps moyen et max passé sur une page en une seule fois
    val avgMaxTimeDF = jsonDF.selectExpr("avg(viewtime) as avg_time_spent", "max(viewtime) as max_time_spent")

    // Afficher les résultats
    val query1 = avgTimeDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    val query2 = maxTimeDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    val query3 = avgMaxTimeDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    query1.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()
  }

}
