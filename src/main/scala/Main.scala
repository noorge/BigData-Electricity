import org.apache.spark.sql.SparkSession
import rdd.RDDOperations
import sql.SQLOperations

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("BigData Project - RDD Operations")
      .master("local[*]")
      .getOrCreate()

    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    spark.sparkContext.setLogLevel("ERROR")

    // Run only Phase 3: RDD operations
//    RDDOperations.run(spark)
    // Run Phase 4: SQL operations
    SQLOperations.run(spark)
    spark.stop()
  }
}