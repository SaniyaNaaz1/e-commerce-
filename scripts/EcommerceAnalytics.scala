// E-Commerce Analytics Project
// Author: Saniya Naaz
// Tool: Apache Spark
// Description: Analyze sales data to calculate revenue and insights
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EcommerceAnalytics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ecommerce Big Data Analytics")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load data
    val df = spark.read.option("header","true").option("inferSchema","true").csv("data/sales.csv")

    // Clean data
    val cleaned = df.filter("price IS NOT NULL AND price >= 0 AND quantity > 0").na.fill("Unknown", Seq("product"))

    // Revenue calculation
    val revenueDF = cleaned.withColumn("revenue", $"price" * $"quantity")

    // Category revenue
    val categoryRevenue = revenueDF.groupBy("category").agg(sum("revenue").alias("total_revenue"))

    // Top 3 products by revenue
    val topProducts = revenueDF.groupBy("product").agg(sum("revenue").alias("total_revenue")).orderBy(desc("total_revenue")).limit(3)
    
    // Save outputs
    categoryRevenue.coalesce(1).write.mode("overwrite").option("header","true").csv("output/day6_category_revenue")
    topProducts.coalesce(1).write.mode("overwrite").option("header","true").csv("output/day6_top_products")

    spark.stop()
  }
}

