// Databricks notebook source
 val secret = dbutils.secrets.get(scope = "day3-scope", key = "sqldbpassword")
print(secret)

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config = Config(Map(
  "url"            -> "rpdbserver.database.windows.net",
  "databaseName"   -> "rpsqldb",
  "dbTable"        -> "dbo.Customers",
  "user"           -> "rpdbserver",
  "password"       -> secret,
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection = spark.read.sqlDB(config)

collection.printSchema
collection.createOrReplaceTempView("customers")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM customers

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config = Config(Map(
  "url"            -> "rpdbserver.database.windows.net",
  "databaseName"   -> "rpsqldb",
  "dbTable"        -> "dbo.Locations",
  "user"           -> "rpdbserver",
  "password"       -> secret,
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection = spark.read.sqlDB(config)

collection.printSchema
collection.createOrReplaceTempView("locations")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM Locations

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC WHERE L.State IN ( "AP", "TN", "KA" )

// COMMAND ----------

// MAGIC     %fs
// MAGIC 
// MAGIC ls /mnt/dlsdata/salesfiles/

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val fileNames = "/mnt/dlsdata/salesfiles/*.csv"
val schema = StructType(
Array(
StructField("SaleId",IntegerType,true),
StructField("SaleDate",IntegerType,true),
StructField("CustomerId",DoubleType,true),
StructField("EmployeeId",DoubleType,true),
StructField("StoreId",DoubleType,true),
StructField("ProductId",DoubleType,true),
StructField("NoOfUnits",DoubleType,true),
StructField("SaleAmount",DoubleType,true),
StructField("SalesReasonId",DoubleType,true),
StructField("ProductCost",DoubleType,true)
)
)

val data = spark.read.option("inferSchema",false).option("header","true").option("sep",",").schema(schema).csv(fileNames)

data.printSchema
data.createOrReplaceTempView("factsales")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TEMP VIEW ProcessedResults
// MAGIC AS
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country,
// MAGIC   S.SaleAmount, S.NoOfUnits
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC INNER JOIN factsales S on S.CustomerId = C.CustomerId
// MAGIC WHERE L.State in ( "AP", "TN" )

// COMMAND ----------

 val results = spark.sql("SELECT * FROM ProcessedResults")

results.printSchema
results.write.mode("append").parquet("/mnt/dlsdata/parque/sales")

// COMMAND ----------

val processedSales = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("/mnt/dlsdata/parque/sales")
processedSales.printSchema
processedSales.show(100, false)


// COMMAND ----------

val dwDatabase = "rpsqldw"
	val dwServer = "rpdbserver"
	val dwUser = "rpdbserver"
	val dwPass = secret
	val dwJdbcPort =  "1433"
	val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

	spark.conf.set(
		"spark.sql.parquet.writeLegacyFormat",
		"true")


// COMMAND ----------

val blobStorage = "trainingstorageaccountrp.blob.core.windows.net"
	val blobContainer = "data"
	val blobAccessKey =  "hQBVChPTZtoxDl4ysvezdr4QxJq2Ir5x+/k3AKKv/r79PaBXNSs1YGS3WrT8tHFDPlqIGQLwQkL654fP1dbScA=="
	val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"


val acntInfo = "fs.azure.account.key."+ blobStorage
sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

results.write
    .format("com.databricks.spark.sqldw")
    .option("url", sqlDwUrlSmall) 
    .option("dbtable", "ProcessedResults")
    .option("forward_spark_azure_storage_credentials","True")
    .option("tempdir", tempDir)
    .mode("overwrite")
    .save()

// COMMAND ----------

