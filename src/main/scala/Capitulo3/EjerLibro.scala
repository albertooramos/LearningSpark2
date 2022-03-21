package Capitulo3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, expr}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object EjerLibro extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  //Crear dataframe
  val df = sparkSession.createDataFrame(Seq(("Brooke", 20),("Brooke", 25), ("Denny", 31), ("Jules", 30), ("RDT", 35)))
    .toDF("name", "age")

  val avgDF = df.groupBy("name").agg(avg("age"))

  avgDF.show()


  //Definir schema

  val schema = StructType(Array(
    StructField("Id", IntegerType, false),
    StructField("First", StringType, false),
    StructField("Last", StringType, false),
    StructField("Url", StringType, false),
    StructField("Published", StringType, false),
    StructField("Hits", IntegerType, false),
    StructField("Campaigns", ArrayType(StringType), false)
  ))

  val blogsDF = sparkSession.read.schema(schema).json("src/main/resources/Capitulo3/blogs.json")

  blogsDF.show(false)

  println(blogsDF.printSchema)
  println(blogsDF.schema)


  //COLUMNS

  blogsDF.columns
  // Access a particular column with col and it returns a Column type
  blogsDF.col("Id")

  // Use an expression to compute a value
  blogsDF.select(expr("Hits * 2")).show(2)
  // or use col to compute value
  blogsDF.select(col("Hits") * 2).show(2)

  // Use an expression to compute big hitters for blogs
  // This adds a new column, Big Hitters, based on the conditional expression
  blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()


  //ROWS

}
