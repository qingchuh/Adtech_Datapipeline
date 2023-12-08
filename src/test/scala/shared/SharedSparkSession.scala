// shared/SharedSparkSession.scala
package shared

import org.apache.spark.sql.SparkSession

trait SharedSparkSession {
  implicit val spark: SparkSession = SparkSession.builder
    .appName("test-app")
    .master("local[2]")  // Use "local" for a single-core local Spark session
    .getOrCreate()
}
