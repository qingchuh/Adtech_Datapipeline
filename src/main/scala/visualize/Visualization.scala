package visualize

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class Visualization(spark: SparkSession) {
  import spark.implicits._

  def showDataFrame(data: DataFrame, numRows: Int = 20): Unit = {
    // Display the first numRows rows of the DataFrame
    data.show(numRows)
  }

  def showFirstNRows(data: DataFrame, n: Int = 10): Unit = {
    // Display the first N rows of the DataFrame
    data.limit(n).show()
  }

  def showColumns(data: DataFrame, columns: String*): Unit = {
    // Display selected columns of the DataFrame
    data.select(columns.map(col): _*).show()
  }

  // Additional methods for more visualizations or operations

}
