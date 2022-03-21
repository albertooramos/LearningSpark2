package Capitulo3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, to_timestamp}
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructField, StructType}

object Fire_calls extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val sampleDF = sparkSession
    .read
    .option("samplingRatio", 0.001)
    .option("header", true)
    .csv("src/main/resources/Capitulo3/sf-fire-calls.csv")



  val fireSchema = StructType(Array(
    StructField("CallNumber", IntegerType, true),
    StructField("UnitID", StringType, true),
    StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true),
    StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true),
    StructField("CallFinalDisposition", StringType, true),
    StructField("AvailableDtTm", StringType, true),
    StructField("Address", StringType, true),
    StructField("City", StringType, true),
    StructField("Zipcode", IntegerType, true),
    StructField("Battalion", StringType, true),
    StructField("StationArea", StringType, true),
    StructField("Box", StringType, true),
    StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true),
    StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true),
    StructField("CallTypeGroup", StringType, true),
    StructField("NumAlarms", IntegerType, true),
    StructField("UnitType", StringType, true),
    StructField("UnitSequenceInCallDispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true),
    StructField("SupervisorDistrict", StringType, true),
    StructField("Neighborhood", StringType, true),
    StructField("Location", StringType, true),
    StructField("RowID", StringType, true),
    StructField("Delay", FloatType, true)
  ))

  val sfFireFile="src/main/resources/Capitulo3/sf-fire-calls.csv"
  val fireDF = sparkSession.read.schema(fireSchema)
    .option("header", "true")
    .csv(sfFireFile)

  val fewFireDF = fireDF
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType").notEqual("Medical Incident"))

  fewFireDF.show(5, false)

  //Devuelve el número de tipos distintos de llamadas mediante countDistinct()
  fireDF
    .select("CallType")
    .where(col("CallType").isNotNull)
    .agg(countDistinct("CallType") as "DistinctCallTypes")
    .show()


  //Podemos enumerar los distintos tipos de llamadas en el conjunto de datos
  fireDF
    .select("CallType")
    .where(col("CallType").isNotNull)
    .distinct()
    .show(10, false)

  //
  val fireTsDF = fireDF
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
      "MM/dd/yyyy hh:mm:ss a"))
    .drop("AvailableDtTm")

  fireTsDF
    .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
    .show(5, false)

  //Guardar en distintos formatos
  fireDF.write.format("csv")
    .mode("overwrite")
    .save("src/main/resources/salida/fire_calls_csv")

  fireDF.write.format("json")
    .mode("overwrite")
    .save("src/main/resources/salida/fire_calls_json")

  fireDF.write.format("avro")
    .mode("overwrite")
    .save("src/main/resources/salida/fire_calls_avro")


}
