package sql

import org.apache.spark.sql.SparkSession

object SQLOperations {

  def run(spark: SparkSession): Unit = {

    // -----------------------------
    // 1. Load dataset as DataFrame
    // -----------------------------
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transformed/transformed_household_power.csv")

    println("\n==============================")
    println("DATAFRAME LOADED")
    println("==============================")
    df.printSchema()

    // -----------------------------
    // 2. Register as SQL view
    // -----------------------------
    df.createOrReplaceTempView("power_data")

    println("\n==============================")
    println("TEMP VIEW CREATED: power_data")
    println("==============================")

    // -----------------------------
    // 3. SQL QUERY 1
    // Average consumption per hour
    // -----------------------------
    println("\n==============================")
    println("SQL QUERY 1: Average Consumption by Hour")
    println("==============================")

    val query1 = spark.sql("""
      SELECT
        hour_of_day,
        AVG(avg_Global_active_power) AS avg_power
      FROM power_data
      GROUP BY hour_of_day
      ORDER BY avg_power DESC
    """)

    query1.show(10, false)
  }
}