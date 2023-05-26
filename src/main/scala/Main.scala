import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();
    val datafile = spark.read
      .format("com.databricks.spark.csv")
      .option("header", true)
      .load("C:/Users/albych/Documents/archive(4)/avocado.csv")
      datafile.show()
    datafile.createOrReplaceTempView("avocado")
    // AveragePrice in all regions for each date.
    spark.sql("select Date, avg(AveragePrice) from avocado group by Date order by Date").show()
    // AveragePrice in all regions for each year.
    spark.sql("select year, avg(AveragePrice) from avocado group by year order by year").show()
    // Average price for each year for each region.
    spark.sql("select year, region, avg(AveragePrice) from avocado group by year, region order by year, region").show()
    // AveragePrice for each regions for all years.
    spark.sql("select region, avg(AveragePrice) from avocado group by region order by region").show()
    // Minimal price in a date across periods and regions.
    spark.sql("select min(AveragePrice) from avocado").show()
    // Total volume sold for each year.
    spark.sql("select year, sum(TotalVolume) from avocado group by year order by year").show()
    // Total volume of conventional and organic avocados sold.
    spark.sql("select type, sum(4046+4225+4770) as Total_type_volume from avocado group by type order by type").show()
    // Total number of non-organic small/medium Hass Avocados sold.
    spark.sql("select sum(4046) as num_of_small_medium from avocado").show()
    // Total number of non-organic large Hass Avocados sold.
    spark.sql("select sum(4225) as num_of_large from avocado").show()
    // Total number of non-organic extra large Hass Avocados sold.
    spark.sql("select sum(4770) as num_of_extra_large from avocado").show()
    spark.stop()
  }
}