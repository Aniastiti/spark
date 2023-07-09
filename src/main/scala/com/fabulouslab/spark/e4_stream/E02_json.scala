package com.fabulouslab.spark.e04_stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}


object E02_json {

  def main(args: Array[String]) {

    /**
      *   Toujours sur le flux du topic pageviews :
      *   - Déserialiser la valeur JSON en Struct
      *   - Construire un dataframe à partir des attributs du Struct
      * */

    // Créer une session Spark
    val spark = SparkSession.builder()
      .appName("JSON Processing")
      .master("local[2]")
      .getOrCreate()

    // Définir le schéma de la structure JSON
    val jsonSchema = StructType(Seq(
      StructField("timestamp", LongType, nullable = true),
      StructField("user", StringType, nullable = true),
      StructField("page", StringType, nullable = true),
      StructField("click", BooleanType, nullable = true)
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

    // Afficher le DataFrame résultant
    val query = jsonDF.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

}
