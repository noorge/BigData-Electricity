package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Cleaning {

  // Keep columns list in one place so all files reuse it
  val numCols: Seq[String] = Seq(
    "Global_active_power",
    "Global_reactive_power",
    "Voltage",
    "Global_intensity",
    "Sub_metering_1",
    "Sub_metering_2",
    "Sub_metering_3"
  )

  // ----------------------------
  // 1) Load raw data (all strings)
  // ----------------------------
  def loadRaw(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "false")
      .csv(path)
  }

  // ----------------------------
  // 2) Replace "?" with null
  // ----------------------------
  def replaceQuestionMarksWithNull(df: DataFrame): DataFrame = {
    df.na.replace(df.columns, Map("?" -> null))
  }

  // ----------------------------
  // 3) Create DateTime column
  // ----------------------------
  def addDateTime(df: DataFrame): DataFrame = {
    df.withColumn(
      "DateTime",
      to_timestamp(concat_ws(" ", col("Date"), col("Time")), "d/M/yyyy H:mm:ss")
    )
  }

  // ----------------------------
  // 4) BEFORE stats (for reporting)
  // ----------------------------
  case class BeforeStats(
                          rowsBefore: Long,
                          badDateTime: Long,
                          missingAnyBefore: Long,
                          missingAllBefore: Long,
                          dupTimestampsBefore: Long,
                          gapRowsBefore: Long
                        )

  def computeBeforeStats(df: DataFrame): BeforeStats = {
    val rowsBefore = df.count()

    val badDT = df.filter(col("DateTime").isNull).count()

    val missingAnyBefore = df.filter(numCols.map(c => col(c).isNull).reduce(_ || _)).count()
    val missingAllBefore = df.filter(numCols.map(c => col(c).isNull).reduce(_ && _)).count()

    val dupTimestampsBefore = df.groupBy(col("DateTime")).count().filter(col("count") > 1).count()

    val w = Window.orderBy(col("DateTime"))
    val gapRows = df
      .select(col("DateTime"))
      .where(col("DateTime").isNotNull)
      .withColumn("prev_dt", lag(col("DateTime"), 1).over(w))
      .withColumn("gap_sec", unix_timestamp(col("DateTime")) - unix_timestamp(col("prev_dt")))
      .filter(col("prev_dt").isNotNull && col("gap_sec") =!= 60)
      .count()

    BeforeStats(rowsBefore, badDT, missingAnyBefore, missingAllBefore, dupTimestampsBefore, gapRows)
  }

  // ----------------------------
  // 5) Drop rows with missing measurements
  // ----------------------------
  def dropMissingMeasurements(df: DataFrame): DataFrame = {
    df.na.drop("any", numCols)
  }

  // ----------------------------
  // 6) Cast numeric columns to double
  // ----------------------------
  def castNumericsToDouble(df: DataFrame): DataFrame = {
    numCols.foldLeft(df) { (acc, c) =>
      acc.withColumn(c, col(c).cast("double"))
    }
  }

  def countCastFailures(df: DataFrame): Long = {
    numCols.map(c => df.filter(col(c).isNull).count()).sum
  }

  // ----------------------------
  // 7) Remove duplicates (full rows based on DateTime + measurements)
  // ----------------------------
  def countFullRowDups(df: DataFrame): Long = {
    val keyCols = Seq("DateTime") ++ numCols
    df.groupBy(keyCols.map(col): _*).count().filter(col("count") > 1).count()
  }

  def dropFullRowDups(df: DataFrame): DataFrame = {
    val keyCols = Seq("DateTime") ++ numCols
    df.dropDuplicates(keyCols)
  }

  // ----------------------------
  // 8) Outlier report (IQR) - print only
  // ----------------------------
  def printOutlierReportIQR(df: DataFrame): Unit = {
    println("\n=== Outlier report (IQR method) ===")
    val n = df.count().toDouble

    def outlierReportIQR(colName: String): Unit = {
      val qs = df.stat.approxQuantile(colName, Array(0.25, 0.75), 0.001)
      val q1 = qs(0)
      val q3 = qs(1)
      val iqr = q3 - q1
      val lower = q1 - 1.5 * iqr
      val upper = q3 + 1.5 * iqr

      val outCount = df.filter(col(colName) < lower || col(colName) > upper).count()
      val pct = outCount * 100.0 / n

      println(f"$colName%-22s outliers=$outCount%8d ($pct%6.3f%%)  bounds=[$lower%.4f,$upper%.4f]")
    }

    numCols.foreach(outlierReportIQR)
  }

  // ----------------------------
  // 9) Save as CSV (single part)
  // ----------------------------
  def saveAsSingleCsv(df: DataFrame, outPath: String): Unit = {
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outPath)
  }
}
