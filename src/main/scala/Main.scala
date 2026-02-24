import org.apache.spark.sql.SparkSession
import preprocessing.{Cleaning, Reduction, Transformation}

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("BigData Project - Cleaning and Reduction")
      .master("local[*]")
      .getOrCreate()

    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    spark.sparkContext.setLogLevel("ERROR")

    // ----------------------------
    // 1) Load + initial preprocessing
    // ----------------------------
    val rawPath = "data/raw/household_power_consumption.txt"

    val df0 = Cleaning.loadRaw(spark, rawPath)
    val df1 = Cleaning.replaceQuestionMarksWithNull(df0)
    val df2 = Cleaning.addDateTime(df1).cache()

    // ----------------------------
    // 2) BEFORE stats
    // ----------------------------
    val stats = Cleaning.computeBeforeStats(df2)

    // ----------------------------
    // 3) Cleaning pipeline
    // ----------------------------
    val dfClean = Cleaning.dropMissingMeasurements(df2)
    val dfTyped = Cleaning.castNumericsToDouble(dfClean)

    val castFailures = Cleaning.countCastFailures(dfTyped)

    val dupBefore = Cleaning.countFullRowDups(dfTyped)
    val dfNoDup   = Cleaning.dropFullRowDups(dfTyped)

    val rowsAfter        = dfNoDup.count()
    val rowsAfterMissing = dfClean.count()

    // ----------------------------
    // 4) Outliers (report only)
    // ----------------------------
    Cleaning.printOutlierReportIQR(dfNoDup)

    // ----------------------------
    // 5) Cleaning report
    // ----------------------------
    println("\n==============================")
    println("   DATA CLEANING SUMMARY")
    println("==============================")

    println(s"Rows BEFORE cleaning: ${stats.rowsBefore}")
    println(s"Bad DateTime rows   : ${stats.badDateTime}")

    println(s"Missing rows (ANY measurement null) BEFORE : ${stats.missingAnyBefore}")
    println(s"Missing rows (ALL measurements null) BEFORE: ${stats.missingAllBefore}")
    println(s"Rows removed due to missing values         : ${stats.rowsBefore - rowsAfterMissing}")

    println(s"Duplicate timestamps BEFORE: ${stats.dupTimestampsBefore}")
    println(s"Time gap rows (missing minutes) BEFORE: ${stats.gapRowsBefore}")

    println(s"Cast failures (nulls after cast) : $castFailures")

    println(s"Duplicate full rows BEFORE dropDuplicates: $dupBefore")
    println(s"Rows removed due to duplicates           : ${rowsAfterMissing - rowsAfter}")

    println(s"Rows AFTER cleaning: $rowsAfter")

    // ----------------------------
    // 6) Save cleaned data (single CSV)
    // ----------------------------
    val outPathClean = "data/cleaned/cleaned_household_power"

    Cleaning.saveAsSingleCsv(dfNoDup, outPathClean)(spark)
    println(s"\nSaved cleaned dataset to: ${outPathClean}.csv")

    // ----------------------------
    // 7) Data reduction (sampling + aggregation + feature selection)
    // ----------------------------
    val outPathReduced = "data/reduced/reduced_household_power"

    val dfReduced = Reduction.reduceAndSave(dfNoDup, outPathReduced)(spark)

    println(s"Rows AFTER reduction: ${dfReduced.count()}")
    println(s"Saved reduced dataset to: ${outPathReduced}.csv")

    // ----------------------------
    // 8) Transformation (type conversions + numeric encoding)
    // ----------------------------
    implicit val implicitSpark: SparkSession = spark

    val outPathTransformed = "data/transformed/transformed_household_power"

    val dfTransformed =
      Transformation.transformAndSave(dfReduced, outPathTransformed)

    println(s"Rows AFTER transformation: ${dfTransformed.count()}")
    println(s"Saved transformed dataset to: ${outPathTransformed}.csv")

    // cleanup
    df2.unpersist()
    spark.stop()
  }
}