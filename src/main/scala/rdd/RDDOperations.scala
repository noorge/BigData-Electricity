package rdd

import org.apache.spark.sql.SparkSession

object RDDOperations {

  def run(spark: SparkSession): Unit = {

    val sc = spark.sparkContext

    // Load transformed dataset
    val data = sc.textFile("data/transformed/transformed_household_power.csv")

    // Remove header
    val header = data.first()
    val rows = data.filter(line => line != header)

    // -----------------------------
    // 1. FILTER (Transformation)
    // -----------------------------
    val highConsumption = rows.filter(line => {
      val cols = line.split(",")
      cols(2).toDouble > 5.0
    })

    println("\n==============================")
    println("RDD Transformation: FILTER")
    println("==============================")
    println("Filtered rows where Global Active Power > 5 kW")

    // -----------------------------
    // 2. COUNT (Action)
    // -----------------------------
    val highCount = highConsumption.count()

    println("\n==============================")
    println("RDD Action: COUNT")
    println("==============================")
    println(s"Number of high consumption records (>5 kW): $highCount")

    // -----------------------------
    // 3. TAKE (Action)
    // -----------------------------
    println("\n==============================")
    println("RDD Action: TAKE (Sample Rows)")
    println("==============================")

    highConsumption.take(5).zipWithIndex.foreach {
      case (row, i) =>
        val cols = row.split(",")
        println(s"Row ${i + 1}: Time=${cols(0)}, Power=${cols(1)}")
    }

  }
}