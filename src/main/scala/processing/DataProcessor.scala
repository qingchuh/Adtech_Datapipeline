package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class DataProcessor(spark: SparkSession) {

  import spark.implicits._

  def aggregateImpressionsBySiteAndAdType(cleanedData: DataFrame): DataFrame = {
    // Aggregating total impressions by site_id and ad_type_id
    cleanedData
      .groupBy("site_id", "ad_type_id")
      .agg(sum("total_impressions").alias("total_impressions"))
  }

  def calculateAverageRevenuePerAdvertiser(cleanedData: DataFrame): DataFrame = {
    // Calculating average revenue per advertiser_id
    cleanedData
      .groupBy("advertiser_id")
      .agg(round(avg("total_revenue"), 1).alias("average_revenue"))
  }

  def analyzeRevenueShareByMonetizationChannel(cleanedData: DataFrame): DataFrame = {
    // Analyzing revenue share across different monetization_channel_ids
    val windowSpec = Window.partitionBy("monetization_channel_id")

    val result = cleanedData
      .withColumn("total_revenue_channel", round(sum("total_revenue").over(windowSpec),1))
      .withColumn("revenue_share_percent", round(col("total_revenue") / col("total_revenue_channel") * 100,1))
      .select("monetization_channel_id", "total_revenue_channel","revenue_share_percent") // Keep only the desired columns

    // Return the result DataFrame
    result
  }
}
