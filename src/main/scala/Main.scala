import org.apache.spark.sql.SparkSession
import preprocessing.Cleaning

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("BigData Project - Data Cleaning")
      .master("local[*]")
      .getOrCreate()

    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    spark.sparkContext.setLogLevel("ERROR")

    // ----------------------------
    // 1) Load + initial preprocessing
    // ----------------------------
    val rawPath = "data/household_power_consumption.txt"

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
    val dfNoDup = Cleaning.dropFullRowDups(dfTyped)

    val rowsAfter = dfNoDup.count()
    val rowsAfterMissing = dfClean.count()

    // ----------------------------
    // 4) Outliers (report only)
    // ----------------------------
    Cleaning.printOutlierReportIQR(dfNoDup)

    // ----------------------------
    // 5) Report
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
    // 6) Save
    // ----------------------------
    val outPath = "file:///C:/Users/Noor/IdeaProjects/BigData-Electricity/data/cleaned/household_power"

    Cleaning.saveAsSingleCsv(dfNoDup, outPath)

    println(s"\nSaved cleaned dataset to: $outPath")

    // cleanup
    df2.unpersist()
    spark.stop()
  }
}
