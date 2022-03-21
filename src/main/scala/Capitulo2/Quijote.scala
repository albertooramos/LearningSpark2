package Capitulo2

import org.apache.spark.sql.SparkSession

object Quijote extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val leer = sparkSession.read.text("src/main/resources/Capitulo2/el_quijote.txt")


  leer.show()
  leer.show(10, false)

  println(leer.count())

  println(leer.head())
  println(leer.take(1))
  println(leer.first())


}
