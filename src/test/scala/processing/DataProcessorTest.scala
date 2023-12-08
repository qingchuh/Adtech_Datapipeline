package processing

import org.apache.spark.sql.{DataFrame,Row,SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType,StringType,DoubleType}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import shared.SharedSparkSession

class DataProcessorTest extends AnyFunSuite with SharedSparkSession {

  val schema = StructType(Seq(
    StructField("date", StringType, nullable = false),
    StructField("site_id", IntegerType, nullable = false),
    StructField("ad_type_id", IntegerType, nullable = false),
    StructField("geo_id", IntegerType, nullable = false),
    StructField("device_category_id", IntegerType, nullable = false),
    StructField("advertiser_id", IntegerType, nullable = false),
    StructField("order_id", IntegerType, nullable = false),
    StructField("line_item_type_id", IntegerType, nullable = false),
    StructField("os_id", IntegerType, nullable = false),
    StructField("integration_type_id", IntegerType, nullable = false),
    StructField("monetization_channel_id", IntegerType, nullable = false),
    StructField("ad_unit_id", IntegerType, nullable = false),
    StructField("total_impressions", IntegerType, nullable = false),
    StructField("total_revenue", DoubleType, nullable = false),
    StructField("viewable_impressions", IntegerType, nullable = false),
    StructField("measurable_impressions", IntegerType, nullable = false),
    StructField("revenue_share_percent", IntegerType, nullable = false)
  ))

  // Sample input data
  val dataS = Seq(
    Row("2019-06-30 00:00:00", 351, 10, 187, 2, 84, 3473, 19, 60, 1, 4, 5174, 16, 4.0, 2, 16, 1),
    Row("2019-06-30 00:00:00", 351, 10, 187, 2, 84, 3473, 19, 58, 1, 4, 5174, 6, 2.0, 0, 6, 1),
    Row("2019-06-30 00:00:00", 351, 10, 147, 2, 84, 3473, 19, 60, 1, 4, 5174, 4, 3.0, 0, 4, 1)
  )

  val inputData = spark.createDataFrame(spark.sparkContext.parallelize(dataS), schema)


  test("aggregateImpressionsBySiteAndAdType should aggregate total impressions by site_id and ad_type_id") {
    val spark = this.spark
    import spark.implicits._

    // Create an instance of DataProcessor
    val dataProcessor = new processing.DataProcessor(spark)

    // Apply the aggregateImpressionsBySiteAndAdType method
    val resultData = dataProcessor.aggregateImpressionsBySiteAndAdType(inputData)

    val expectedDataSeq = Seq(
      (351, 10, 26)
    ).toDF("site_id","ad_type_id","total_impressions")

    assert(resultData.collect() === expectedDataSeq.collect())
  }

  test("calculateAverageRevenuePerAdvertiser should calculate average revenue per advertiser") {
    val spark = this.spark
    import spark.implicits._

    // Create an instance of DataProcessor
    val dataProcessor = new processing.DataProcessor(spark)

    // Apply the calculateAverageRevenuePerAdvertiser method
    val resultData = dataProcessor.calculateAverageRevenuePerAdvertiser(inputData)

    // Expected result based on the sample input data
    val expectedData = Seq(
      (84, 3.0)
    ).toDF("advertiser_id", "average_revenue")


    // Use assert method to check DataFrame equality
    assert(resultData.collect() === expectedData.collect())

  }

  test("analyzeRevenueShareByMonetizationChannel should analyze revenue share by monetization channel") {
    val spark = this.spark
    import spark.implicits._

    // Create an instance of DataProcessor
    val dataProcessor = new processing.DataProcessor(spark)

    // Apply the analyzeRevenueShareByMonetizationChannel method
    val resultData = dataProcessor.analyzeRevenueShareByMonetizationChannel(inputData)

    // Expected result based on the sample input data
    val expectedData = Seq(
      (4, 9.0, 44.4),
      (4, 9.0, 22.2),
      (4, 9.0, 33.3)
    ).toDF("monetization_channel_id", "total_revenue_channel", "revenue_share_percent")
    resultData.show()
    // Use assert method to check DataFrame equality
    assert(resultData.collect() === expectedData.collect())
  }
}
