package upm.bd.group4


import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Normalizer, OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}




/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Spark Practical Work")
    val spark = SparkSession.builder().getOrCreate()
    

    spark.sparkContext.setLogLevel("FATAL")
    import spark.implicits._

     print("\n")
      print("Big Data Spark project group4\n")
      print("Marivicitr Hang\nPhilipp Scomparin\nSili Liu\n")
 

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
      .drop("TailNum")
      .na.drop("any")

    println("FilteredData: "+filteredDF.count() + " rows")
    filteredDF.show(5)
    //   filteredDF.printSchema()

    /*
     println(filteredDF.select("Cancelled").collect().map(_(0)).toList)

     filteredDF.agg(max(filteredDF(filteredDF.columns(10))), min(filteredDF(filteredDF.columns(10)))).show
     filteredDF.agg(max(filteredDF(filteredDF.columns(9))), min(filteredDF(filteredDF.columns(9)))).show
   */

/*
    val indexer = new StringIndexer()
      .setInputCols(Array("UniqueCarrier","Origin","Dest"))
      .setOutputCols(Array("UniqueCarrierIndex","OriginIndex","DestIndex"))
    val indexed = indexer.fit(filteredDF).transform(filteredDF)

    val oneHotEncoder = new OneHotEncoder()
      .setInputCols(Array("UniqueCarrierIndex","OriginIndex","DestIndex"))
      .setOutputCols(Array("UniqueCarrierVec","OriginVec","DestVec"))
      .setDropLast(false)
    val oneHotEncoded = oneHotEncoder.fit(indexed).transform(indexed)

    */


    val categFeatures = Array("UniqueCarrier","Origin","Dest")
    println("Categorical features: " + categFeatures.mkString("Array(", ", ", ")"))
    // categColumns.show(5)




    val encodedFeatures = categFeatures.flatMap{name =>

      val stringIndexer = new StringIndexer()
        .setInputCol(name)
        .setOutputCol(name + "Index")

      val oneHotEncoder = new OneHotEncoder()
        .setInputCols(Array(name + "Index"))
        .setOutputCols(Array(name + "Vec"))
        .setDropLast(false)

      Array(stringIndexer, oneHotEncoder)
    }


    val pipeline = new Pipeline().setStages(encodedFeatures)
    val indexerModel = pipeline.fit(filteredDF)
    val dfTransformed = indexerModel.transform(filteredDF)



    val assembler = new VectorAssembler()
      .setInputCols(Array("UniqueCarrierVec","OriginVec","DestVec","Year","Month","DayOfMonth","DayOfWeek","CRSDepTime","CRSArrTime","CRSElapsedTime","FlightNum","DepDelay","Distance","TaxiOut"))
      .setOutputCol("features")


    val output = assembler.transform(dfTransformed)


    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)
    val NormData = normalizer.transform(output)
    //NormData.select("features","normFeatures").show(false)

    //  NormData.select("features","normFeatures").show(5)
    //  NormData.printSchema()

    val Array(trainingData, testData) = NormData.randomSplit(Array(0.7, 0.3))


    //-------------------LINEAR REGRESSION---------------------------------------------

    val lr = new LinearRegression()
      .setLabelCol("ArrDelay")
      .setFeaturesCol("normFeatures")
      .setPredictionCol("predictionLR")
      .setMaxIter(10)
      .setRegParam(1.0)
      .setElasticNetParam(1.0)

    // Fit the model
    val lrModel = lr.fit(trainingData)

    // print output and input column
    val predictionsLR = lrModel.transform(testData)
    //predictionsLR.select("normFeatures", "ArrDelay", "predictionLR").show()

    val resultLR=predictionsLR.select("normFeatures","ArrDelay", "predictionLR")

    println("-------------------------Linear Regression-------------------------")
    resultLR.show()


    // Print coeff and intercept
    // println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")



    // Summarize the model and print out some metrics
    val trainingSummaryLR = lrModel.summary

    trainingSummaryLR.residuals.show()
    println(s"RMSE = ${trainingSummaryLR.rootMeanSquaredError}")
    println(s"R-Squared = ${trainingSummaryLR.r2}")

    //-------------------GAUSSIAN LINEAR REGRESSION---------------------------------------------

    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .setLabelCol("ArrDelay")
      .setFeaturesCol("normFeatures")
      .setPredictionCol("predictionGLR")
      .setMaxIter(10)
      .setRegParam(1.0)

    // Fit the model
    val glrModel = glr.fit(trainingData)

    // print output and input column
    val predictionsGLR = glrModel.transform(testData)
    println("-------------------------Gaussian Linear Regression-------------------------")
    predictionsGLR.select("ArrDelay", "predictionGLR").show()
    val resultGLR = predictionsGLR.select("normFeatures","ArrDelay", "predictionGLR")


    // Summarize the model and print out some metrics
    val trainingSummaryGLR = glrModel.summary

    trainingSummaryGLR.residuals.show()

    val evaluatorGLR_RMSE = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("predictionGLR")
      .setMetricName("rmse")
      .evaluate(predictionsGLR)

    println(s"Root Mean Squared Error = $evaluatorGLR_RMSE")

    val evaluatorGLR_R2 = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("predictionGLR")
      .setMetricName("r2")
      .evaluate(predictionsGLR)

    println(s"R-Squared = $evaluatorGLR_R2")


   //-------------------Decision tree regression---------------------------------------------

    val dt = new DecisionTreeRegressor()
      .setLabelCol("ArrDelay")
      .setFeaturesCol("normFeatures")
      .setPredictionCol("predictionDT")

    val dtmodel = dt.fit(trainingData)
    val predictionsDT = dtmodel.transform(testData)
    println("-------------------------Decision Tree Regression-------------------------")

    predictionsDT.select("ArrDelay", "predictionDT").show()

    val resultDT = predictionsDT.select("normFeatures","ArrDelay", "predictionDT")

    // Select (prediction, true label) and compute test error.
    val evaluatorDT_RMSE = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("predictionDT")
      .setMetricName("rmse")
      .evaluate(predictionsDT)

    println(s"Root Mean Squared Error = $evaluatorDT_RMSE")

    val evaluatorDT_R2 = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("predictionDT")
      .setMetricName("r2")
      .evaluate(predictionsDT)

    println(s"R-Squared = $evaluatorDT_R2")


    //-------------------RANDOM FOREST REGRESSION---------------------------------------------

        val rf = new RandomForestRegressor()
          .setLabelCol("ArrDelay")
          .setFeaturesCol("normFeatures")
          .setPredictionCol("predictionRF")

        val modelRF = rf.fit(trainingData)

        // Make predictions.
        val predictionsRF = modelRF.transform(testData)

          println("-------------------------Random Forest Regression-------------------------")

        // Select example rows to display.
        predictionsRF.select("ArrDelay", "predictionRF").show()


        val resultRF = predictionsRF.select("normFeatures", "ArrDelay", "predictionRF")


        // Select (prediction, true label) and compute test error.
        val evaluatorRF_RMSE = new RegressionEvaluator()
          .setLabelCol("ArrDelay")
          .setPredictionCol("predictionRF")
          .setMetricName("rmse")
          .evaluate(predictionsRF)

        println(s"Root Mean Squared Error = $evaluatorRF_RMSE")

        val evaluatorRF_R2 = new RegressionEvaluator()
          .setLabelCol("ArrDelay")
          .setPredictionCol("predictionRF")
          .setMetricName("r2")
          .evaluate(predictionsRF)

        println(s"R-Squared = $evaluatorRF_R2")


    //------------------------------------------Summaries-----------------------------------------

    val result_aux1 = resultGLR
      .withColumnRenamed("normFeatures","norm")
      .withColumnRenamed("ArrDelay","Arr")
    val result_aux2 = resultLR.join(result_aux1,resultLR("normFeatures")===result_aux1("norm")&&resultLR("ArrDelay")===result_aux1("Arr"))
    val result_aux3 = result_aux2
      .drop("norm")
      .drop("Arr")

    val result_aux4 = resultRF
      .withColumnRenamed("normFeatures","norm")
      .withColumnRenamed("ArrDelay","Arr")
    val result_aux5 = resultDT.join(result_aux4,resultDT("normFeatures")===result_aux4("norm")&&resultDT("ArrDelay")===result_aux4("Arr"))
    val result_aux6 = result_aux5
      .drop("normFeatures")
      .drop("ArrDelay")

    val result = result_aux3.join(result_aux6,result_aux3("normFeatures")===result_aux6("norm")&&result_aux3("ArrDelay")===result_aux6("Arr"))
    result
      .drop("norm")
      .drop("Arr")
      .show()

    val summaryDF = Seq(
      ("LINEAR REGRESSION",trainingSummaryLR.rootMeanSquaredError,trainingSummaryLR.r2),
      ("GAUSSIAN LINEAR REGRESSION",evaluatorGLR_RMSE,evaluatorGLR_R2),
      ("DECISION TREE REGRESSION",evaluatorDT_RMSE,evaluatorDT_R2),
      ("RANDOM FOREST REGRESSION",evaluatorRF_RMSE,evaluatorRF_R2)
    ).toDF("Algorithm","RMSE","R2")

    summaryDF.show(false)


/*


*/

  }

}