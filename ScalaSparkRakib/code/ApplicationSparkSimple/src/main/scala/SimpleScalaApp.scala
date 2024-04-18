import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, desc}

object SimpleScalaApp extends App {
  val spark = SparkSession.builder.master("local[*]").appName("SimpleScalaApp").getOrCreate
//  spark.sparkContext.setLogLevel("ERROR")

  val taxi = spark.read.option("inferschema", "true").option("header", "true").parquet("yellow_tripdata_2024-01.parquet")

  val simpleSelect = taxi.select("VendorID", "passenger_count")
  val passengerCountGT2 = taxi.filter(col("passenger_count") > 2)
  val passengerPayment2 = taxi.where(col("payment_type") === 2)
  val avgAmount =  taxi.agg(avg("total_amount").alias("average_total_amount"))
  val longestDistance = taxi.sort(desc("trip_distance")).limit(1)
  val paymentType =  taxi.groupBy("payment_type").count()
  val distance10Asc = taxi.where(col("trip_distance") > 10).orderBy("trip_distance")
  val tipGT20PC = taxi.where(col("tip_amount") / col("fare_amount") > 0.20)
  val shortDistance = taxi.where(col("trip_distance") < 1 && col("total_amount") > 10)
  val amountNull = taxi.where(col("total_amount") === 0)

}
