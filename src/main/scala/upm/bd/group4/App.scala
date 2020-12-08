package upm.bd.group4

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._


    val df = spark.read.format("csv").load("*.csv")
    .withColumnRenamed("_c0", "year")
    .withColumnRenamed("_c1", "month")
    .withColumnRenamed("_c2", "dayOfMonth")
    .withColumnRenamed("_c3", "dayOfWeek")
    .withColumnRenamed("_c4", "depTime")
    .withColumnRenamed("_c5", "CRSDepTime")
    .withColumnRenamed("_c6", "arrTime")
    .withColumnRenamed("_c7", "CRSArrTime")
    .withColumnRenamed("_c8", "uniqueCarrier")
    .withColumnRenamed("_c9", "flightNum")
    .withColumnRenamed("_c10", "tailNum")
    .withColumnRenamed("_c11", "actualElapsedTime")
    .withColumnRenamed("_c12", "CRSElapsedTime")
    .withColumnRenamed("_c13", "airTime")
    .withColumnRenamed("_c14", "arrDelay")
    .withColumnRenamed("_c15", "depDelay")
    .withColumnRenamed("_c16", "origin")
    .withColumnRenamed("_c17", "dest")
    .withColumnRenamed("_c18", "distance")
    .withColumnRenamed("_c19", "taxiIn")
    .withColumnRenamed("_c20", "taxiOut")
    .withColumnRenamed("_c21", "cancelled")
    .withColumnRenamed("_c22", "cancellationCode")
    .withColumnRenamed("_c23", "diverted")
    .withColumnRenamed("_c24", "carrierDelay")
    .withColumnRenamed("_c25", "weatherDelay")
    .withColumnRenamed("_c26", "NASDelay")
    .withColumnRenamed("_c27", "securityDelay")
    .withColumnRenamed("_c28", "lateAircraftDelay")

    df.printSchema()

 
  }

}
