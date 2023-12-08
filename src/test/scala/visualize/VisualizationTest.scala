package visualize


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite


class VisualizationTest extends AnyFunSuite {
  private val spark: SparkSession = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
  private val visualization: Visualization = new Visualization(spark)

  // Assuming you have a test DataFrame named 'testDataFrame'
  import spark.implicits._
  private val testDataFrame: DataFrame = Seq(
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 22)
  ).toDF("id", "name", "age")

  test("showDataFrame should display the DataFrame") {
    visualization.showDataFrame(testDataFrame, numRows = 5)
  }

  test("showFirstNRows should display the first N rows") {
    visualization.showFirstNRows(testDataFrame, n = 2)
  }

  test("showColumns should display selected columns") {
    visualization.showColumns(testDataFrame, "id", "name")
  }

}
