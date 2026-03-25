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

    // -----------------------------
    // SQL QUERY 2: Statistical Summary
    // -----------------------------
    println("\n==============================")
    println("SQL QUERY 2: Statistical Summary of Electricity Consumption")
    println("==============================")

    val query2 = spark.sql("""
  SELECT
    COUNT(DISTINCT date) AS unique_days,
    AVG(avg_Global_active_power) AS mean_power,
    VARIANCE(avg_Global_active_power) AS variance_power
  FROM power_data
   """)

    query2.show(false)

  // -----------------------------
// SQL QUERY 3: Rank Years by Average Power Consumption
// -----------------------------
println("\n==============================")
println("SQL QUERY 3: Rank Years by Average Power Consumption")
println("==============================")

val query3 = spark.sql("""
  SELECT 
    year,
    avg_power,
    RANK() OVER (ORDER BY avg_power DESC) AS power_rank
  FROM (
    SELECT 
      YEAR(date) AS year,
      AVG(avg_Global_active_power) AS avg_power
    FROM power_data
    GROUP BY YEAR(date)
  ) yearly_data
  ORDER BY power_rank
""")

query3.show(false)
// -----------------------------
// SQL QUERY 4: Identify Days with Above-Average Consumption
// -----------------------------
println("\n==============================")
println("SQL QUERY 4: Identify Days with Above-Average Consumption")
println("==============================")

val query4 = spark.sql("""
  WITH daily_consumption AS (
    SELECT 
      date,
      SUM(avg_Global_active_power) AS total_power
    FROM power_data
    GROUP BY date
  )
  
  SELECT 
    date,
    total_power
  FROM daily_consumption
  WHERE total_power > (
    SELECT AVG(total_power) FROM daily_consumption
  )
  ORDER BY total_power DESC
""")

query4.show(10, false)
  }
}