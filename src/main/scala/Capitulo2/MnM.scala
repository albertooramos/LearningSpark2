package Capitulo2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc, max, min}

object MnM extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val leer = sparkSession.read.option("header", "true").csv("src/main/resources/Capitulo2/mnmdata.csv")

  val maxMnM = leer
    .select("State", "Color", "Count")
    .groupBy("State", "Color")
    .agg(max("Count").alias("Max"))
    .orderBy(desc("Max")).show()


  val nvMnM = leer
    .select("State", "Color", "Count")
    .where(col("State") === "NV")
    .groupBy("State", "Color")
    .agg(count("Color").alias("Colors"))
    .orderBy(desc("Colors")).show()


  val operMnM = leer.select("State", "Color", "Count")
    .groupBy("State","Color")
    .agg(count("Count").alias("Count"),
      max("Count").alias("Max"),
      min("Count").alias("Min")).show








}
