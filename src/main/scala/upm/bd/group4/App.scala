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

    df.printSchema()
  }

}
