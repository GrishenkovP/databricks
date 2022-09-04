// Databricks notebook source
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType, DateType}
import spark.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{udf, col, lit, round, when}
import org.apache.spark.sql.functions.{sum, count, avg, min, max}
import org.apache.spark.sql.functions.{substring}

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("dbfs:/FileStore/tables/")

// COMMAND ----------

val spark = SparkSession.builder().master("local[*]").appName("SparkScalaExamples")getOrCreate()
println(spark)

// COMMAND ----------

// MAGIC %md ###Create DataFrame

// COMMAND ----------

val schemaDfSales = StructType( Array(
                 StructField("key_id", IntegerType, true),
                 StructField("dt", DateType, true),
                 StructField("region", StringType, true),
                 StructField("manager", StringType, true),
                 StructField("product", StringType, true),
                 StructField("val", IntegerType, true)
             ))

// COMMAND ----------

val dfSales = spark.read.format("csv")
      .option("header", "true")
      .options(Map("delimiter"->","))
      .schema(schemaDfSales)
      .load("dbfs:/FileStore/tables/data.csv")

// COMMAND ----------

dfSales.printSchema()

// COMMAND ----------

dfSales.show(false)

// COMMAND ----------

val dataRegions = Seq(("r1","cluster1"),
                      ("r2","cluster1"),
                      ("r3","cluster1"),
                      ("r4","cluster2"),
                      ("r5","cluster2"),
                      ("r6","cluster2"),
                      ("r7","cluster3"),
                      ("r8","cluster3"),
                      ("r9","cluster3"),
                      ("r10","cluster4")
  )
val columnsRegions = Seq("region","cluster")

val dfRegions = dataRegions.toDF(columnsRegions:_*)

// COMMAND ----------

dfRegions.show(false)

// COMMAND ----------

val dfClusters = dfRegions.select(col("cluster").alias("cluster_name"),col("region"))

// COMMAND ----------

dfClusters.show(false)

// COMMAND ----------

// MAGIC %md ###Select Columns

// COMMAND ----------

val columnsDfClusters=dfClusters.columns.map(m=>col(m))

// COMMAND ----------

dfClusters.select(columnsDfClusters:_*).show()

// COMMAND ----------

val listColsDfSales= List("key_id","dt", "val")
dfSales.select(listColsDfSales.map(m=>col(m)):_*).show()

// COMMAND ----------

dfSales.select(dfSales.columns.slice(0,3).map(m=>col(m)):_*).show()

// COMMAND ----------

// MAGIC %md ###Add and Update Column (withColumn)

// COMMAND ----------

val dfClustersNew = dfClusters.withColumn("testCol", lit("-"))

// COMMAND ----------

dfClustersNew.show()

// COMMAND ----------

val dfSalesNew = dfSales.withColumn("percent",round(col("val")/100*5,2))

// COMMAND ----------

dfSalesNew.show()

// COMMAND ----------

// MAGIC %md ### Drop column

// COMMAND ----------

dfClustersNew.drop("testCol").printSchema()

// COMMAND ----------

// MAGIC %md ### Rename Nested Column

// COMMAND ----------

dfClustersNew.withColumnRenamed("testCol","testColNew").printSchema()

// COMMAND ----------

// MAGIC %md ### Where | Filter

// COMMAND ----------

dfSalesNew.filter(col("region") === "r1" && col("manager") === "m1").show(false)

// COMMAND ----------

// MAGIC %md ### When Otherwise

// COMMAND ----------

val dfSalesResult = dfSalesNew.withColumn("group", when(col("percent") <= 1,"gr1").when(col("percent") > 1 && col("percent") <2,"gr2").otherwise("gr3"))

// COMMAND ----------

dfSalesResult.show()

// COMMAND ----------

// MAGIC %md ### Distinct

// COMMAND ----------

println("Distinct count: "+ dfSalesResult.select("region","manager","product").distinct().count())

// COMMAND ----------

println("Distinct count: "+ dfSalesResult.select("region","manager","product").dropDuplicates().count())

// COMMAND ----------

// MAGIC %md ### Pivot Table DataFrame

// COMMAND ----------

val pivotDfSales = dfSalesResult.groupBy("region").pivot("manager").sum("val")
pivotDfSales.show()

// COMMAND ----------

// MAGIC %md ### Groupby

// COMMAND ----------

val groupbyDfSales = dfSalesResult.groupBy("product")
                                  .agg(
                                    sum("val").as("sum_val"),
                                    count("val").as("count_val"),
                                    avg("val").as("avg_val"),
                                    min("val").as("min_val"),
                                    max("val").as("max_val"))
groupbyDfSales.show(false)

// COMMAND ----------

// MAGIC %md ### Sort DataFrame

// COMMAND ----------

dfSalesResult.sort(col("region").asc,col("manager").asc).show(false)

// COMMAND ----------

dfSalesResult.orderBy(col("region").asc,col("manager").asc).show(false)

// COMMAND ----------

// MAGIC %md ### Join Types

// COMMAND ----------

dfSales.show(false)

// COMMAND ----------

dfRegions.show(false)

// COMMAND ----------

  val joinDf = dfSales.join(dfRegions,dfSales("region") ===  dfRegions("region"),"left")
  joinDf.show(false)

// COMMAND ----------

// MAGIC %md ### UDF (User Defined Functions)

// COMMAND ----------

val funcString =  (currentStr:String) => {
    currentStr.substring(1,2)}

// COMMAND ----------

 val funcStringUDF = udf(funcString)
 dfSales.select(col("region"), funcStringUDF(col("region")).as("number_region") ).show(false)

// COMMAND ----------

// MAGIC %md ### SQL

// COMMAND ----------

dfSalesNew.createOrReplaceTempView("tblSales")

// COMMAND ----------

spark.sql("""select s.region, 
                 s.manager, 
                 sum(s.val) as total_val 
          from tblSales as s
          group by s.region, 
                   s.manager
          order by sum(s.val) desc""").show(false)
