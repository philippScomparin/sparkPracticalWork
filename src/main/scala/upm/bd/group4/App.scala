package upm.bd.group4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    import spark.implicits._


    val wholeData = spark.read.format("csv").load("*.csv")
    .withColumnRenamed("_c0", "Year")
    .withColumnRenamed("_c1", "Month")
    .withColumnRenamed("_c2", "DayOfMonth")
    .withColumnRenamed("_c3", "DayOfWeek")
    .withColumnRenamed("_c4", "DepTime")
    .withColumnRenamed("_c5", "CRSDepTime")
    .withColumnRenamed("_c6", "ArrTime")
    .withColumnRenamed("_c7", "CRSArrTime")
    .withColumnRenamed("_c8", "UniqueCarrier")
    .withColumnRenamed("_c9", "FlightNum")
    .withColumnRenamed("_c10", "TailNum")
    .withColumnRenamed("_c11", "ActualElapsedTime")
    .withColumnRenamed("_c12", "CRSElapsedTime")
    .withColumnRenamed("_c13", "AirTime")
    .withColumnRenamed("_c14", "ArrDelay")
    .withColumnRenamed("_c15", "DepDelay")
    .withColumnRenamed("_c16", "Origin")
    .withColumnRenamed("_c17", "Dest")
    .withColumnRenamed("_c18", "Distance")
    .withColumnRenamed("_c19", "TaxiIn")
    .withColumnRenamed("_c20", "TaxiOut")
    .withColumnRenamed("_c21", "Cancelled")
    .withColumnRenamed("_c22", "CancellationCode")
    .withColumnRenamed("_c23", "Diverted")
    .withColumnRenamed("_c24", "CarrierDelay")
    .withColumnRenamed("_c25", "WeatherDelay")
    .withColumnRenamed("_c26", "NASDelay")
    .withColumnRenamed("_c27", "SecurityDelay")
    .withColumnRenamed("_c28", "LateAircraftDelay")
    .withColumn("Cancelled", col("Cancelled").cast("boolean"))

    val allowedData = wholeData.drop("ArrTime")
    .drop("ActualElapsedTime")
    .drop("AirTime")
    .drop("TaxiIn")
    .drop("Diverted")
    .drop("CarrierDelay")
    .drop("WeatherDelay")
    .drop("NASDelay")
    .drop("SecurityDelay")
    .drop("LateAircraftDelay")


    println("before deleting" + allowedData.count())

    val workingData = allowedData.filter(col("CRSElapsedTime") > 0)
    .filter(col("Cancelled")===false)
    .withColumn("Date", concat(col("DayOfMonth"), lit("/"), col("Month"), lit("/"), col("Year")))
    .withColumn("Date", to_date(unix_timestamp($"Date", "dd/MM/yyyy")))
    .drop("Year")
    .drop("Month")
    .drop("DayOfMonth")
    println("elapsed deleted" + workingData.count())
    workingData.show()


 
  }

}
