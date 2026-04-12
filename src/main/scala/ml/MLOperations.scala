package ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{VectorAssembler, MinMaxScaler}
import org.apache.spark.ml.regression.LinearRegression

object MLOperations {

  def run(spark: SparkSession): Unit = {

    // Load dataset
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transformed/transformed_household_power.csv")

    // Split data
    val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed = 42)

    // -----------------------------
    // 1. FEATURE ENGINEERING
    // -----------------------------
    println("\n==============================")
    println("ML Step 1: Feature Engineering")
    println("==============================")

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "avg_Voltage",
        "avg_Global_intensity",
        "total_sub_metering"
      ))
      .setOutputCol("features")

    val trainAssembled = assembler.transform(trainDF)
    val testAssembled = assembler.transform(testDF)

    println("Features combined into vector (features column)")
    trainAssembled.select("features").show(5)

    // -----------------------------
    // 2. SCALING
    // -----------------------------
    println("\n==============================")
    println("ML Step 2: Min-Max Scaling")
    println("==============================")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val scalerModel = scaler.fit(trainAssembled)

    val finalTrain = scalerModel.transform(trainAssembled)
    val finalTest = scalerModel.transform(testAssembled)

    println("Scaled features (range 0 to 1)")
    finalTrain.select("scaledFeatures").show(5)

    // -----------------------------
    // 3. MODEL TRAINING
    // -----------------------------
    println("\n==============================")
    println("ML Step 3: Model Training")
    println("==============================")

    val lr = new LinearRegression()
      .setLabelCol("avg_Global_active_power")
      .setFeaturesCol("scaledFeatures")
      .setPredictionCol("prediction")

    val lrModel = lr.fit(finalTrain)

    println("Linear Regression model trained successfully")
    println(s"Intercept: ${lrModel.intercept}")
    println(s"Number of coefficients: ${lrModel.coefficients.size}")

    // -----------------------------
    // 4. MODEL EVALUATION
    // -----------------------------
    println("\n==============================")
    println("ML Step 4: Model Evaluation")
    println("==============================")

    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.sql.functions._

    // Generate predictions on test set
    val predictions = lrModel.transform(finalTest)

    println("Sample predictions:")
    predictions
      .select("avg_Global_active_power", "prediction")
      .show(10, truncate = false)

    // Evaluators
    val rmseEvaluator = new RegressionEvaluator()
      .setLabelCol("avg_Global_active_power")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val maeEvaluator = new RegressionEvaluator()
      .setLabelCol("avg_Global_active_power")
      .setPredictionCol("prediction")
      .setMetricName("mae")

    val r2Evaluator = new RegressionEvaluator()
      .setLabelCol("avg_Global_active_power")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    val testRMSE = rmseEvaluator.evaluate(predictions)
    val testMAE = maeEvaluator.evaluate(predictions)
    val testR2 = r2Evaluator.evaluate(predictions)

    println(f"Test RMSE: $testRMSE%.4f")
    println(f"Test MAE : $testMAE%.4f")
    println(f"Test R²  : $testR2%.4f")

    // -----------------------------
    // 5. BASELINE COMPARISON
    // -----------------------------
    println("\n==============================")
    println("ML Step 5: Baseline Comparison")
    println("==============================")

    // Baseline = predict mean of training label for all test rows
    val trainMeanLabel = finalTrain
      .agg(avg("avg_Global_active_power"))
      .first()
      .getDouble(0)

    val baselinePredictions = finalTest.withColumn("prediction", lit(trainMeanLabel))

    val baselineRMSE = rmseEvaluator.evaluate(baselinePredictions)
    val baselineMAE = maeEvaluator.evaluate(baselinePredictions)
    val baselineR2 = r2Evaluator.evaluate(baselinePredictions)

    println(f"Baseline mean prediction: $trainMeanLabel%.4f")
    println(f"Baseline RMSE: $baselineRMSE%.4f")
    println(f"Baseline MAE : $baselineMAE%.4f")
    println(f"Baseline R²  : $baselineR2%.4f")

    // -----------------------------
    // 6. COMPARISON SUMMARY
    // -----------------------------
    println("\n==============================")
    println("Evaluation Summary")
    println("==============================")

    if (testRMSE < baselineRMSE) {
      println("The Linear Regression model outperforms the baseline in RMSE.")
    } else {
      println("The Linear Regression model does not outperform the baseline in RMSE.")
    }

    if (testMAE < baselineMAE) {
      println("The Linear Regression model outperforms the baseline in MAE.")
    } else {
      println("The Linear Regression model does not outperform the baseline in MAE.")
    }

    if (testR2 > baselineR2) {
      println("The Linear Regression model explains more variance than the baseline.")
    } else {
      println("The Linear Regression model does not explain more variance than the baseline.")
    }

    val featureNames = Array("avg_Voltage", "avg_Global_intensity", "total_sub_metering")
println("\nModel Coefficients (on scaled features):")
featureNames.zip(lrModel.coefficients.toArray).foreach {
  case (name, coef) => println(f"  $name%-30s -> $coef%.6f")
}
println(f"  Intercept: ${lrModel.intercept}%.6f")
  }
}