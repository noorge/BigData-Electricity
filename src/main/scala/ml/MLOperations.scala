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
  }
}