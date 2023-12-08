// ConfigReaderTest.scala
package configuration

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}

import java.nio.file.{Files, Path, Paths}

class ConfigReaderTest extends AnyFunSuite {

  // data schema test
  test("ConfigReader should load data schema from application.conf") {
    val spark: SparkSession = SparkSession.builder.master("local").appName("ConfigReaderTest").getOrCreate()
    val configReader = new ConfigReader(spark)

    // Validate data schema
    assert(configReader.dataSchema.length == 17)
    assert(configReader.dataSchema.map(_.name) == List(
      "date", "site_id", "ad_type_id", "geo_id", "device_category_id",
      "advertiser_id", "order_id", "line_item_type_id", "os_id",
      "integration_type_id", "monetization_channel_id", "ad_unit_id",
      "total_impressions", "total_revenue", "viewable_impressions",
      "measurable_impressions", "revenue_share_percent"
    ))

    // Validate data types (customize based on your actual data types)
    assert(configReader.dataSchema.map(_.dataType) == List(
      TimestampType, IntegerType, IntegerType, IntegerType, IntegerType,
      IntegerType, IntegerType, IntegerType, IntegerType, IntegerType,
      IntegerType, IntegerType, IntegerType, DoubleType, IntegerType,
      IntegerType, IntegerType
    ))
  }

  // file reading test
  test("ConfigReader should load input file paths and output directory") {
    val spark: SparkSession = SparkSession.builder.master("local").appName("ConfigReaderTest").getOrCreate()
    val configReader = new ConfigReader(spark)

    // Validate input file paths
    assert(configReader.inputFilePaths.length == 1)
    configReader.inputFilePaths.foreach { path =>
      assert(Files.exists(path))
    }

    // Validate output directory
    assert(Files.exists(configReader.outputDirectoryPath))
  }

}
