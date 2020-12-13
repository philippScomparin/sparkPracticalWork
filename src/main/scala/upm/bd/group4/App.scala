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
    .withColumn("CRSElapsedTime", col("CRSElapsedTime").cast("integer"))
    

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

    def convertMinutesToHour (totalMinutes : Int) = {
      val hours : Int = totalMinutes/60
      val minutes : Int = totalMinutes-hours*60
      (hours, minutes)
    }

    def convertHoursToMinutes (totalHours : String) = {
      var hours = 0
      if (totalHours.length == 4){
        hours = totalHours.take(2).toInt
      }
      else {
        hours = totalHours.take(1).toInt 
      }

      val minutes = totalHours.takeRight(2).toInt

      hours * 60 + minutes
    }

    val addMinutes = udf((time : String, minutes : Int) => {
      val minutesOfTime = convertHoursToMinutes(time)
      val totalMinutes = minutesOfTime + minutes
      val (crsHours : Int, crsMinutes : Int) = convertMinutesToHour(totalMinutes)
      if (crsMinutes < 10){
        val crsMinutesWith0 = ("0" + crsMinutes.toString)
        (crsHours.toString + crsMinutesWith0)
      }
      else{
      (crsHours.toString + crsMinutes.toString)
      }

    })

    

    val workingData = allowedData.filter(col("CRSElapsedTime") > 0)
    .filter(col("Cancelled")===false)
    .withColumn("Date", concat(col("Year"), lit("-"), col("Month"), lit("-"), col("DayOfMonth")))
    .withColumn("Date", to_date(col("Date")))
    .drop("Year")
    .drop("Month")
    .drop("DayOfMonth")
    .withColumn("CRSArrTime", addMinutes(col("CRSDepTime"), col("CRSElapsedTime")))
    .drop("DepTime")
    .drop("Cancelled")
    .drop("CancellationCode")


    workingData.show()
    workingData.printSchema()


    


 
  }

}
