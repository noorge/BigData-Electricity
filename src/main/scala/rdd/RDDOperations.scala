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
      cols(1).toDouble > 5.0
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

    
      // -----------------------------
// 4. MAP (Transformation)
// -----------------------------
val hourPowerPairs = rows.map(line => {
  val cols = line.split(",")

  val hour = cols(8).toInt              // hour_of_day
  val power = cols(1).toDouble          // avg_Global_active_power

  (hour, power)
})

println("\n==============================")
println("RDD Transformation: MAP")
println("==============================")
println("Mapped each record into key-value pairs: (hour, power)")

println("Sample records after MAP:")
hourPowerPairs.take(3).zipWithIndex.foreach {
  case ((hour, power), i) =>
    println(f"Sample ${i + 1}: Hour=$hour%02d, Power=$power%.4f")
}


// -----------------------------
// 5. GROUPBYKEY (Transformation)
// -----------------------------
val groupedHourlyConsumption = hourPowerPairs.groupByKey()

println("\n==============================")
println("RDD Transformation: GROUPBYKEY")
println("==============================")
println("Grouped electricity consumption values by hour")

// ✅ Clean sample (only show first value instead of full list)
println("Sample records after GROUPBYKEY:")
groupedHourlyConsumption.take(3).zipWithIndex.foreach {
  case ((hour, values), i) =>
    println(f"Sample ${i + 1}: Hour=$hour%02d, Example_Power=${values.head}%.4f")
}


// Convert to totals (so analysis still works)
val hourlyConsumption = groupedHourlyConsumption.map {
  case (hour, values) => (hour, values.sum)
}


// -----------------------------
// 6. REDUCE (Action)
// -----------------------------
println("\n==============================")
println("RDD Action: REDUCE")
println("==============================")
println("Identified the hour with the highest total electricity consumption")

// ✅ Direct final result (no confusing samples)
val maxConsumptionHour = hourlyConsumption.reduce {
  case ((hour1, total1), (hour2, total2)) =>
    if (total1 > total2) (hour1, total1) else (hour2, total2)
}

println(f"Result: Hour=${maxConsumptionHour._1}%02d, Total_Power=${maxConsumptionHour._2}%.4f")
    // -----------------------------
   // 7. MAP (Transformation)
   // -----------------------------
   val categorizedUsage = rows.map(line => {
   val cols = line.split(",")
   val power = cols(1).toDouble
   val category =
     if (power < 1) "Low"
     else if (power < 3) "Medium"
     else "High"
     (category, 1)
   })
   println("\n==============================")
   println("RDD Transformation: MAP")
   println("==============================")
   println("Categorized electricity usage into (Low, Medium, High)")
   // -----------------------------
   // 8. REDUCEBYKEY (Transformation)
   // -----------------------------
   val categoryCounts = categorizedUsage.reduceByKey(_ + _)
   println("\n==============================")
   println("RDD Transformation: REDUCEBYKEY")
   println("==============================")
   println("Counted records in each usage category")
   // -----------------------------
   // 9. COLLECT (Action)
   // -----------------------------
   println("\n==============================")
   println("RDD Action: COLLECT")
   println("==============================")
   categoryCounts
     .collect()
     .zipWithIndex
     .foreach {
    case ((category, total), i) =>
      println(s"Row ${i + 1}: Category=$category, Total_Records=$total")
  }
    // -----------------------------
    // 10. MAP (Transformation)
    // -----------------------------
    val datePowerPairs = rows.map(line => {
      val cols = line.split(",")

      val date = cols(7)                 // date
      val power = cols(1).toDouble       // avg_Global_active_power

      (date, power)
    })

    println("\n==============================")
    println("RDD Transformation: MAP")
    println("==============================")
    println("Mapped each record into key-value pairs: (date, power)")

    println("\nSample output after MAP:")
    datePowerPairs.take(5).zipWithIndex.foreach {
      case ((date, power), i) =>
        println(f"Row ${i + 1}: Date=$date, Power=$power%.4f")
    }

    // -----------------------------
    // 11. REDUCEBYKEY (Transformation)
    // -----------------------------
    val dailyConsumption = datePowerPairs.reduceByKey(_ + _)

    println("\n==============================")
    println("RDD Transformation: REDUCEBYKEY")
    println("==============================")
    println("Aggregated total electricity consumption per day")

    println("\nSample output after REDUCEBYKEY:")
    dailyConsumption.take(5).zipWithIndex.foreach {
      case ((date, totalPower), i) =>
        println(f"Row ${i + 1}: Date=$date, Total_Power=$totalPower%.4f")
    }

    // -----------------------------
    // 12. SORTBYKEY (Transformation)
    // -----------------------------
    val rankedDays = dailyConsumption
      .map { case (date, totalPower) => (totalPower, date) }
      .sortByKey(ascending = false)

    println("\n==============================")
    println("RDD Transformation: SORTBYKEY")
    println("==============================")
    println("Ranked days by total electricity consumption in descending order")
    println("\nSample output after SORTBYKEY:")
    rankedDays.take(5).zipWithIndex.foreach {
      case ((totalPower, date), i) =>
        println(f"Row ${i + 1}: Date=$date, Total_Power=$totalPower%.4f")
    }

    // -----------------------------
    // 13. FIRST (Action)
    // -----------------------------
    println("\n==============================")
    println("RDD Action: FIRST")
    println("==============================")

    val highestDay = rankedDays.first()
    println(f"Highest consumption day: Date=${highestDay._2}, Total_Power=${highestDay._1}%.4f")

    // -----------------------------
    // 14. TAKE (Action)
    // -----------------------------
    println("\n==============================")
    println("RDD Action: TAKE")
    println("==============================")
    println("Top 5 days with highest electricity consumption:")

    rankedDays.take(5).zipWithIndex.foreach {
      case ((totalPower, date), i) =>
        println(f"Rank ${i + 1}: Date=$date, Total_Power=$totalPower%.4f")
    }
    }
}