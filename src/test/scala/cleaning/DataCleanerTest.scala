package cleaning


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.scalatest.funsuite.AnyFunSuite

class DataCleanerTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder.master("local").appName("test").getOrCreate()
  import spark.implicits._

  val schema = StructType(Seq(
    StructField("date", StringType, nullable = true),
    StructField("value1", IntegerType, nullable = true),
    StructField("value2", StringType, nullable = true)
  ))

  val dataCleaner = new DataCleaner(spark)

  test("removeMissingValues should remove rows with missing values") {
    val data = Seq(
      Row("2022-01-01", 1, "A"),
      Row("2022-01-02", 2, null), // leave null values as-is
      Row("2022-01-03", null, "C") // leave null values as-is
    )

    val rawData = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val cleanedData: DataFrame = dataCleaner.removeMissingValues(rawData)
    assert(cleanedData.count() == 1)
    assert(cleanedData.collect()(0) == Row("2022-01-01", 1, "A"))
  }


  test("removeDuplicates should remove duplicate rows") {
    val data = Seq(
      Row("2022-01-01", 1, "A"),
      Row("2022-01-02", 2, "B"),
      Row("2022-01-01", 1, "A")
    )

    val rawData = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Apply removeDuplicates method
    val cleanedData = dataCleaner.removeDuplicates(rawData)

    // Assert the result
    val expectedData = Seq(
      ("2022-01-01", 1, "A"),
      ("2022-01-02", 2, "B")
    ).toDF("date", "value1", "value2")

    // Use assert method to check DataFrame equality
    assert(cleanedData.collect() === expectedData.collect())
  }
}
