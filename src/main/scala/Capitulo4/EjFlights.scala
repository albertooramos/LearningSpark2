package Capitulo4

import org.apache.spark.sql.SparkSession


object EjFlights extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val leer = sparkSession.read.option("header", true).option("inferSchema","true").csv("src/main/resources/Capitulo4")

  // Create a temporary view
  leer.createTempView("us_delay_flights_tbl")

  sparkSession.sql("""SELECT distance, origin, destination
                    FROM us_delay_flights_tbl WHERE distance > 1000
                    ORDER BY distance DESC""").show(10)

  sparkSession.sql("""SELECT date, delay, origin, destination
                    FROM us_delay_flights_tbl
                    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
                    ORDER by delay DESC""").show(10)

  sparkSession.sql("""SELECT delay, origin, destination,
                     CASE
                     WHEN delay > 360 THEN 'Very Long Delays'
                     WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                     WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                     WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                     WHEN delay = 0 THEN 'No Delays'
                     ELSE 'Early'
                     END AS Flight_Delays
                     FROM us_delay_flights_tbl
                     ORDER BY origin, delay DESC""").show(10)

  //Leer formatos

  sparkSession.read.format("json").load("src/main/resources/salida/fire_calls_json").show()
  sparkSession.read.format("avro").load("src/main/resources/salida/fire_calls_avro").show()
  sparkSession.read.format("parquet").load("src/main/resources/salida/fire_calls_parquet").show()






}
