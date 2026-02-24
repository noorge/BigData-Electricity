package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

object Transformation {

  /** 1) Add time-derived columns */
  def addTimeDerivedColumns(df: DataFrame): DataFrame = {
    df
      .withColumn("date", to_date(col("Hour")))
      .withColumn("hour_of_day", hour(col("Hour")))
  }

  /**
   * 2) Manual One-Hot Encoding for:
   *    - Day of week (7 columns)
   *    - Month (12 columns)
   *
   * dayofweek(): 1=Sunday ... 7=Saturday
   */
  def oneHotEncodeDaysMonths(df: DataFrame): DataFrame = {

    val dow = dayofweek(col("Hour"))   // 1..7
    val mon = month(col("Hour"))       // 1..12

    // Create 7 day columns: dow_1 ... dow_7
    val withDow = (1 to 7).foldLeft(df) { (acc, d) =>
      acc.withColumn(s"dow_$d", when(dow === d, 1.0).otherwise(0.0))
    }

    // Create 12 month columns: month_1 ... month_12
    val withMon = (1 to 12).foldLeft(withDow) { (acc, m) =>
      acc.withColumn(s"month_$m", when(mon === m, 1.0).otherwise(0.0))
    }

    withMon
  }

  /** Full transformation pipeline */
  def transform(df: DataFrame): DataFrame = {
    val withTime = addTimeDerivedColumns(df)
    oneHotEncodeDaysMonths(withTime)
  }

  /** Save as single CSV file */
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
    fs.delete(finalPath, false)
    fs.rename(partFile, finalPath)

    fs.delete(new Path(tempDir), true)
  }

  /** Transform + Save */
  def transformAndSave(df: DataFrame, outPath: String)
                      (implicit spark: SparkSession): DataFrame = {
    val transformed = transform(df)
    saveAsSingleCsv(transformed, outPath)
    transformed
  }
}