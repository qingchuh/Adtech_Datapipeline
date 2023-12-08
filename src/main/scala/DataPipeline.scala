import cleaning.DataCleaner
import configuration.ConfigReader
import processing.DataProcessor
import visualize.Visualization
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataPipeline {
  def main(args: Array[String]): Unit = {

    // Start measuring time
    val startTime = System.currentTimeMillis()


    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("datapipeline")
      .master("local[2]")
      .getOrCreate()


    // Load configuration
    val configReader = new ConfigReader(spark)

    // Load and clean data
    val rawData = configReader.loadData(configReader.inputFilePaths, configReader.dataSchema)
    val cleanedData = new DataCleaner(spark).cleanData(rawData)

    // Process data
    val dataProcessor = new DataProcessor(spark)
    val aggregatedData = dataProcessor.aggregateImpressionsBySiteAndAdType(cleanedData)
    val averageRevenueData = dataProcessor.calculateAverageRevenuePerAdvertiser(cleanedData)
    val revenueShareData = dataProcessor.analyzeRevenueShareByMonetizationChannel(cleanedData)

    // Visualize data
    val visualization = new Visualization(spark)
    visualization.showFirstNRows(aggregatedData, 10)
    visualization.showFirstNRows(averageRevenueData, 10)
    visualization.showFirstNRows(revenueShareData, 10)

    // Save processed data to the output directory
    val outputDirectoryPath: String = configReader.outputDirectoryPath.toString
    aggregatedData.write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$outputDirectoryPath/aggregated_data.csv")
    averageRevenueData.write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$outputDirectoryPath/average_revenue_data.csv")
    revenueShareData.write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$outputDirectoryPath/revenue_share_data.csv")

    // Perform additional visualizations or save data as needed

    // Stop measuring time
    val endTime = System.currentTimeMillis()
    // Calculate and print the total time spent
    val totalTime = (endTime - startTime) / 1000.0
    println(s"Total time spent: $totalTime seconds")

    // Stop Spark session
    spark.stop()
  }
}
