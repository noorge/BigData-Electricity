package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

object Reduction {

  // 1) Aggregate minute-level data to hourly averages
  def aggregateHourly(df: DataFrame): DataFrame = {

    val withHour =
      df.withColumn("Hour", date_trunc("hour", col("DateTime")))

    val numCols = Cleaning.numCols

    val aggExprs =
      numCols.map(c => avg(col(c)).alias(s"avg_$c"))

    withHour
      .groupBy("Hour")
      .agg(aggExprs.head, aggExprs.tail: _*)
      .orderBy("Hour")
  }

  // 2) Sample AFTER aggregation (sample hours, not minutes)
  def sampleHours(df: DataFrame, fraction: Double = 0.5): DataFrame = {
    df.sample(withReplacement = false, fraction, seed = 42L)
  }

  // 3) Feature selection
  def selectFeatures(df: DataFrame): DataFrame = {

    df.select(
      col("Hour"),
      col("avg_Global_active_power"),
      col("avg_Voltage"),
      col("avg_Global_intensity"),
      col("avg_Sub_metering_1"),
      col("avg_Sub_metering_2"),
      col("avg_Sub_metering_3")
    )
  }

  // 4) Full reduction pipeline + save as single CSV (no folder)
  def reduceAndSave(df: DataFrame, outPath: String)
                   (implicit spark: SparkSession): DataFrame = {

    val hourly  = aggregateHourly(df)
    val sampled = sampleHours(hourly)
    val reduced = selectFeatures(sampled)

    val tempDir = outPath + "_temp"

    // Write to temporary folder
    reduced.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(tempDir)

    // Use Hadoop API to rename part file to a single CSV
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val partFile = fs.listStatus(new Path(tempDir))
      .map(_.getPath)
      .find(_.getName.startsWith("part-"))
      .get

    // Final clean CSV path (caller passes base path without .csv)
    val finalPath = new Path(outPath + ".csv")
    fs.delete(finalPath, false) // in case it already exists
    fs.rename(partFile, finalPath)

    // Delete temporary folder
    fs.delete(new Path(tempDir), true)

    reduced
  }
}