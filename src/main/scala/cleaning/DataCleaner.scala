// DataCleaner.scala
package cleaning

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

class DataCleaner(spark: SparkSession) {

  def removeMissingValues(rawData: DataFrame): DataFrame = {
    rawData.na.drop()
  }


  def removeDuplicates(dataWithFixedStructuralErrors: DataFrame): DataFrame = {
    dataWithFixedStructuralErrors.dropDuplicates()
  }

  def cleanData(rawData: DataFrame): DataFrame = {
    val dataWithoutMissingValues = removeMissingValues(rawData)
    removeDuplicates(dataWithoutMissingValues)
  }

  // Other cleaning functions can be added based on your specific requirements

}
