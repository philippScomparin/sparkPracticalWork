package upm.bd.group4

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Normalizer, OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor



/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    import spark.implicits._

  /*  print("\n")
    print("Big Data Spark project group4\n")
    print("Marivicitr Hang\nPhilipp Scomparin\nSili Liu\n")
*/

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
      .withColumn("ArrDelay",col("ArrDelay").cast("integer"))
      .withColumn("CRSElapsedTime",col("CRSElapsedTime").cast("integer"))

    println("\nAllowedData: " + allowedData.count() + " rows")


    var filteredDF = allowedData.withColumn("Year", col("Year").cast("integer"))
      .withColumn("Month", col("Month").cast("integer"))
      .withColumn("DayOfMonth", col("DayOfMonth").cast("integer"))
      .withColumn("DayOfWeek", col("DayOfWeek").cast("integer"))
      .withColumn("CRSDepTime", col("CRSDepTime").cast("integer"))
      .withColumn("CRSArrTime", col("CRSArrTime").cast("integer"))
      .withColumn("FlightNum", col("FlightNum").cast("integer"))
      .withColumn("ArrDelay", col("ArrDelay").cast("integer"))
      .withColumn("DepDelay", col("DepDelay").cast("integer"))
      .withColumn("CRSElapsedTime", col("CRSElapsedTime").cast("integer"))
      .withColumn("Cancelled", col("Cancelled").cast("integer"))
      .withColumn("TaxiOut", col("TaxiOut").cast("integer"))
      .withColumn("Distance", col("Distance").cast("integer"))
      .filter("CRSElapsedTime > 0")
      .filter("Cancelled == 0")
      .drop("CancellationCode")
      .drop("Cancelled")
      .drop("DepTime")
      .na.drop()

    println("FilteredData: "+filteredDF.count() + " rows")
    filteredDF.show(5)
 //   filteredDF.printSchema()

   /*
    println(filteredDF.select("Cancelled").collect().map(_(0)).toList)

    filteredDF.agg(max(filteredDF(filteredDF.columns(10))), min(filteredDF(filteredDF.columns(10)))).show
    filteredDF.agg(max(filteredDF(filteredDF.columns(9))), min(filteredDF(filteredDF.columns(9)))).show
  */


    val categFeatures = Array("UniqueCarrier","TailNum","Origin","Dest")
    println("Categorical features: " + categFeatures.mkString("Array(", ", ", ")"))
 //   categColumns.show(5)


    val encodedFeatures = categFeatures.flatMap{name =>

      val stringIndexer = new StringIndexer()
        .setInputCol(name)
        .setOutputCol(name + "Index")

      val oneHotEncoder = new OneHotEncoder()
        .setInputCols(Array(name + "Index"))
        .setOutputCols(Array(name + "Vec"))
        .setDropLast(false)

     Array(stringIndexer, oneHotEncoder)
    //   Array(oneHotEncoder)
    }

    val pipeline = new Pipeline().setStages(encodedFeatures)
    val indexerModel = pipeline.fit(filteredDF)
    val dfTransformed = indexerModel.transform(filteredDF)

  //  dfTransformed.show(5)
  //  dfTransformed.printSchema()


    val assembler = new VectorAssembler()
      .setInputCols(Array("UniqueCarrierVec","TailNumVec","OriginVec","DestVec","Year","Month","DayOfMonth","DayOfWeek","CRSDepTime","CRSArrTime","CRSElapsedTime","FlightNum","DepDelay","Distance","TaxiOut"))
      .setOutputCol("features")
      

 //   assembler.show(3)
 //   assembler.printSchema()

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)
      


  //  normalizer.select("features","normFeatures").show(5)
  //  normalizer.printSchema()

    val Array(trainingData, testData) = dfTransformed.randomSplit(Array(0.7, 0.3))


   //-------------------LINEAR REGRESSION---------------------------------------------

    val lr = new LinearRegression()
      .setLabelCol("ArrDelay")
      .setFeaturesCol("normFeatures")
      .setMaxIter(10)
      .setRegParam(1.0)
      .setElasticNetParam(1.0)


    val pipeline2 = new Pipeline().setStages(Array(assembler, normalizer, lr))
    val lrModel = pipeline2.fit(trainingData)
    val lrResult = lrModel.transform(testData)

    lrResult.select("features", "normFeatures", "ArrDelay", "prediction").show()

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(lrResult)
      println(s"Root Mean Squared Error = $rmse")

    val evaluatorR2 = new RegressionEvaluator()
    .setLabelCol("ArrDelay")
    .setPredictionCol("prediction")
    .setMetricName("r2");
    val r2 = evaluatorR2.evaluate(lrResult);
    println(s"R2 = $r2")

//     // Fit the model
//     val lrModel = lr.fit(trainingData)

//     // print output and input column
//     lrModel.transform(testData).select("features","normFeatures", "ArrDelay", "prediction").show()

//     // Print coeff and intercept
//     println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

//     // Summarize the model and print out some metrics
//     val trainingSummary = lrModel.summary
//  //   println(s"numIterations: ${trainingSummary.totalIterations}")
//   //  println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
//     trainingSummary.residuals.show()
//     println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
//     println(s"r2: ${trainingSummary.r2}")



//------------------------------------------- Decision Tree ------------------------------------------------------------
// Train a DecisionTree model.
// val dt = new DecisionTreeRegressor()
//   .setLabelCol("ArrDelay")
//   .setFeaturesCol("normFeatures")

// // Chain indexer and tree in a Pipeline.
// val pipeline2 = new Pipeline()
//   .setStages(Array(dt))

// // Train model. This also runs the indexer.
// val model = pipeline2.fit(trainingData)

// // Make predictions.
// val predictions = model.transform(testData)

// // Select example rows to display.
// predictions.select("prediction", "ArrDelay", "features").show(5)

// // Select (prediction, true label) and compute test error.
// val evaluator = new RegressionEvaluator()
//   .setLabelCol("ArrDelay")
//   .setPredictionCol("prediction")
//   .setMetricName("rmse")
// val rmse = evaluator.evaluate(predictions)
// println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

// val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
// println(s"Learned regression tree model:\n ${treeModel.toDebugString}")



  }

}