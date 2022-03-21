import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Weblogs extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  var leer = sparkSession.read
    .option("header", "true")
    .option("sep", " ")
    .option("inferSchema", "true")
    .csv("src/main/resources/weblogs/nasa_aug95.csv")

  //leer.show(false)
  //leer.printSchema()

  val prot = leer.withColumn("new_row",split(col("request"), " "))
    .select(col("new_row").getItem(2).as("Protocolos"))
    .filter(col("Protocolos").like("%HT%/%")).distinct().show()

  val stat = leer.groupBy("Status").count()
    .orderBy(desc("Count")).show()

  val pet = leer.withColumn("new_row",split(col("request"), " "))
    .select(col("new_row").getItem(0).as("Peticion"))
    .groupBy("Peticion").count().orderBy(col("count").desc).show()

  val tran = leer.filter(col("response_size").isNotNull)
    .select(col("requesting_host"), col("response_size"))
    .orderBy(col("response_size").desc).limit(1).show()

  val tra = leer.groupBy(to_date(col("datetime"))).count()
    .orderBy(col("count").desc).limit(1).show()

  val hor = leer.groupBy(hour(col("datetime"))).count()
    .orderBy(col("count")).show()

  val err = leer.filter(col("status").equalTo(404))
    .groupBy(to_date(col("datetime"))).count()
    .orderBy(col("count")).show()

}
