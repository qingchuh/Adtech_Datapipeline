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
    cleanedData
      .groupBy("monetization_channel_id")
      .agg(sum("revenue_share_percent").alias("total_revenue_share_percent"))
  }
}
