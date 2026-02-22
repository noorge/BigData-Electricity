package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{FileSystem, Path}

object Cleaning {

  // Numeric columns used across project
  val numCols: Seq[String] = Seq(
    "Global_active_power",
    "Global_reactive_power",
    "Voltage",
    "Global_intensity",
    "Sub_metering_1",
    "Sub_metering_2",
    "Sub_metering_3"
  )

  // 1) Load raw dataset
  def loadRaw(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "false")
      .csv(path)
  }

  // 2) Replace "?" with null
  def replaceQuestionMarksWithNull(df: DataFrame): DataFrame = {
    df.na.replace(df.columns, Map("?" -> null))
  }

  // 3) Create DateTime column
  def addDateTime(df: DataFrame): DataFrame = {
    df.withColumn(
      "DateTime",
      to_timestamp(concat_ws(" ", col("Date"), col("Time")), "d/M/yyyy H:mm:ss")
    )
  }

  // 4) BEFORE statistics
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

    val badDateTime = df.filter(col("DateTime").isNull).count()

    val missingAnyBefore =
      df.filter(numCols.map(c => col(c).isNull).reduce(_ || _)).count()

    val missingAllBefore =
      df.filter(numCols.map(c => col(c).isNull).reduce(_ && _)).count()

    val dupTimestampsBefore =
      df.groupBy("DateTime").count().filter(col("count") > 1).count()

    val w = Window.orderBy(col("DateTime"))

    val gapRowsBefore = df
      .select(col("DateTime"))
      .where(col("DateTime").isNotNull)
      .withColumn("prev_dt", lag(col("DateTime"), 1).over(w))
      .withColumn(
        "gap_sec",
        unix_timestamp(col("DateTime")) - unix_timestamp(col("prev_dt"))
      )
      .filter(col("prev_dt").isNotNull && col("gap_sec") =!= 60)
      .count()

    BeforeStats(
      rowsBefore,
      badDateTime,
      missingAnyBefore,
      missingAllBefore,
      dupTimestampsBefore,
      gapRowsBefore
    )
  }

  // 5) Drop rows with missing numeric measurements
  def dropMissingMeasurements(df: DataFrame): DataFrame = {
    df.na.drop("any", numCols)
  }

  // 6) Cast numeric columns to Double
  def castNumericsToDouble(df: DataFrame): DataFrame = {
    numCols.foldLeft(df) { (acc, c) =>
      acc.withColumn(c, col(c).cast("double"))
    }
  }

  // Count casting failures
  def countCastFailures(df: DataFrame): Long = {
    numCols.map(c => df.filter(col(c).isNull).count()).sum
  }

  // 7) Remove duplicate full rows
  def countFullRowDups(df: DataFrame): Long = {
    val keyCols = Seq("DateTime") ++ numCols
    df.groupBy(keyCols.map(col): _*)
      .count()
      .filter(col("count") > 1)
      .count()
  }

  def dropFullRowDups(df: DataFrame): DataFrame = {
    val keyCols = Seq("DateTime") ++ numCols
    df.dropDuplicates(keyCols)
  }

  // 8) Outlier report using IQR
  def printOutlierReportIQR(df: DataFrame): Unit = {

    println("\n=== Outlier Report (IQR) ===")

    val totalRows = df.count().toDouble

    def report(colName: String): Unit = {

      val quantiles =
        df.stat.approxQuantile(colName, Array(0.25, 0.75), 0.001)

      val q1 = quantiles(0)
      val q3 = quantiles(1)
      val iqr = q3 - q1

      val lower = q1 - 1.5 * iqr
      val upper = q3 + 1.5 * iqr

      val outliers =
        df.filter(col(colName) < lower || col(colName) > upper).count()

      val percent = outliers * 100.0 / totalRows

      println(
        f"$colName%-22s outliers=$outliers%8d ($percent%6.3f%%) bounds=[$lower%.4f,$upper%.4f]"
      )
    }

    numCols.foreach(report)
  }

  // 9) Save as single CSV (no folder)
  def saveAsSingleCsv(df: DataFrame, outPath: String)
                     (implicit spark: SparkSession): Unit = {

    val tempDir = outPath + "_temp"

    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(tempDir)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val partFile = fs.listStatus(new Path(tempDir))
      .map(_.getPath)
      .find(_.getName.startsWith("part-"))
      .get

    val finalPath = new Path(outPath + ".csv")
    fs.delete(finalPath, false) // in case exists
    fs.rename(partFile, finalPath)

    fs.delete(new Path(tempDir), true)
  }
}