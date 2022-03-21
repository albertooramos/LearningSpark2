import org.apache.spark.sql.functions.{lit, sum}
import org.apache.spark.sql.{SparkSession, functions}

object Padron extends App{

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()


  var leer = sparkSession.read
    .option("header", "true")
    .option("sep", ";")
    .option("inferSchema", "true")
    .csv("src/main/resources/Padron/Rango_Edades_Seccion_202203.csv")

  val bar = leer.select(col("DESC_BARRIO")).distinct().show()

  leer.createTempView("padron")
  sparkSession.sql("SELECT COUNT(DISTINCT DESC_BARRIO) FROM padron")

  val long = leer.withColumn("Longitud",functions.length(col("DESC_DISTRITO")))

  val col = long.withColumn("Val5", lit(5))

  val del = col.drop("Val5")

  del.write.mode("overwrite")
    .partitionBy("DESC_DISTRITO","DESC_BARRIO")
    .csv("src/main/resources/Padron/Particionado")


  val numT = del.groupBy(col("DESC_DISTRITO"), col("DESC_BARRIO"))
    .agg(sum(col("EspanolesHombres")), sum(col("EspanolesMujeres")), sum(col("ExtranjerosHombres")),sum(col("ExtranjerosMujeres")))
    .select("DESC_DISTRITO","DESC_BARRIO", "EspanolesHombres", "EspanolesMujeres", "ExtranjerosHombres", "ExtranjerosMujeres")
    .orderBy(col("ExtranjerosMujeres").desc(col("ExtranjerosHombres")))
    .show()

  val uni = del.groupBy(col("DESC_DISTRITO"),col("DESC_BARRIO")).sum("EspanolesHombres")
    .select(col("DESC_DISTRITO"), col("DESC_BARRIO"), col("sum(EspanolesHombres)")).show()



}
