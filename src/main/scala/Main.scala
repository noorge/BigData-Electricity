import org.apache.spark.sql.SparkSession
import ml.MLOperations

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

    MLOperations.run(spark)

    spark.stop()
  }
}