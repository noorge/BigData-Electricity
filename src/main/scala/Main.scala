import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

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
    spark.sparkContext.hadoopConfiguration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    // ----------------------------
    // 1) Load raw data (all strings)
    // ----------------------------
    val df0 = spark.read
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "false")
      .csv("data/household_power_consumption.txt")

    // ----------------------------
    // 2) Replace "?" with null
    // ----------------------------
    val df1 = df0.na.replace(df0.columns, Map("?" -> null))

    // ----------------------------
    // 3) Create DateTime column
    // ----------------------------
    val df2 = df1.withColumn(
      "DateTime",
      to_timestamp(concat_ws(" ", col("Date"), col("Time")), "d/M/yyyy H:mm:ss")
    ).cache()

    val rowsBefore = df2.count()

    // Columns that should be numeric
    val numCols = Seq(
      "Global_active_power",
      "Global_reactive_power",
      "Voltage",
      "Global_intensity",
      "Sub_metering_1",
      "Sub_metering_2",
      "Sub_metering_3"
    )

    // ----------------------------
    // 4) Simple BEFORE statistics
    // ----------------------------
    val badDT = df2.filter(col("DateTime").isNull).count()

    val missingAnyBefore = df2.filter(numCols.map(c => col(c).isNull).reduce(_ || _)).count()
    val missingAllBefore = df2.filter(numCols.map(c => col(c).isNull).reduce(_ && _)).count()

    val dupTimestampsBefore = df2.groupBy(col("DateTime")).count().filter(col("count") > 1).count()

    // Time gaps (missing timestamps) — NOTE: this checks missing minutes, not missing measurement values
    val w = Window.orderBy(col("DateTime"))
    val gapRows = df2
      .select(col("DateTime"))
      .where(col("DateTime").isNotNull)
      .withColumn("prev_dt", lag(col("DateTime"), 1).over(w))
      .withColumn("gap_sec", unix_timestamp(col("DateTime")) - unix_timestamp(col("prev_dt")))
      .filter(col("prev_dt").isNotNull && col("gap_sec") =!= 60)
      .count()

    // ----------------------------
    // 5) Cleaning step: drop rows with missing measurements
    // ----------------------------
    val dfClean = df2.na.drop("any", numCols)

    val rowsAfterMissingDrop = dfClean.count()
    val rowsRemovedMissing = rowsBefore - rowsAfterMissingDrop

    // ----------------------------
    // 6) Standardization: cast to double
    // ----------------------------
    var dfTyped = dfClean
    numCols.foreach { c =>
      dfTyped = dfTyped.withColumn(c, col(c).cast("double"))
    }

    // Verify casting (should be 0 because we dropped missing rows already)
    val castFailures = numCols.map(c => dfTyped.filter(col(c).isNull).count()).sum

    // ----------------------------
    // 7) Remove duplicates (full rows based on DateTime + measurements)
    // ----------------------------
    val keyCols = Seq("DateTime") ++ numCols
    val dupFullRowsBefore = dfTyped.groupBy(keyCols.map(col): _*).count().filter(col("count") > 1).count()

    val dfNoDup = dfTyped.dropDuplicates(keyCols)
    val rowsAfterDupDrop = dfNoDup.count()
    val rowsRemovedDup = rowsAfterMissingDrop - rowsAfterDupDrop

    // ----------------------------
    // 8) Outlier report (IQR) - report only, do not remove
    // ----------------------------
    println("\n=== Outlier report (IQR method) ===")
    val n = rowsAfterDupDrop.toDouble

    def outlierReportIQR(colName: String): Unit = {
      val qs = dfNoDup.stat.approxQuantile(colName, Array(0.25, 0.75), 0.001)
      val q1 = qs(0)
      val q3 = qs(1)
      val iqr = q3 - q1
      val lower = q1 - 1.5 * iqr
      val upper = q3 + 1.5 * iqr

      val outCount = dfNoDup.filter(col(colName) < lower || col(colName) > upper).count()
      val pct = outCount * 100.0 / n

      println(f"$colName%-22s outliers=$outCount%8d ($pct%6.3f%%)  bounds=[$lower%.4f,$upper%.4f]")
    }

    numCols.foreach(outlierReportIQR)

    // ----------------------------
    // 9) Simple BEFORE/AFTER report (what they asked for)
    // ----------------------------
    println("\n==============================")
    println("   DATA CLEANING SUMMARY")
    println("==============================")

    println(s"Rows BEFORE cleaning: $rowsBefore")
    println(s"Bad DateTime rows   : $badDT")

    println(s"Missing rows (ANY measurement null) BEFORE : $missingAnyBefore")
    println(s"Missing rows (ALL measurements null) BEFORE: $missingAllBefore")
    println(s"Rows removed due to missing values         : $rowsRemovedMissing")

    println(s"Duplicate timestamps BEFORE: $dupTimestampsBefore")
    println(s"Time gap rows (missing minutes) BEFORE: $gapRows")

    println(s"Cast failures (nulls after cast) : $castFailures")

    println(s"Duplicate full rows BEFORE dropDuplicates: $dupFullRowsBefore")
    println(s"Rows removed due to duplicates           : $rowsRemovedDup")

    println(s"Rows AFTER cleaning: $rowsAfterDupDrop")

    // ----------------------------
    // 10) Save cleaned dataset as CSV (Windows-safe)
    // ----------------------------

    // Windows/Hadoop local FS tweaks
    spark.sparkContext.hadoopConfiguration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    val outPath = "file:///C:/Users/Noor/IdeaProjects/BigData-Electricity/data/cleaned/household_power"

    dfNoDup
      .coalesce(1) // one CSV part file (easier for submission)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outPath)

    println(s"\nSaved cleaned dataset to: $outPath")


    // cleanup
    df2.unpersist()
    spark.stop()
  }
}
