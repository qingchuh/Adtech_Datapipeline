package configuration

import java.nio.file.{Path, Paths}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType, DataType, TimestampType}

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * Reads configuration parameters for the data pipeline.
 */
class ConfigReader(spark: SparkSession) {
  private val config: Config = ConfigFactory.load()

  // Other configuration properties...

  // List of input files
  val inputFiles: List[String] = config.getStringList("datapipeline.inputFiles").asScala.toList

  // Output directory for processed data
  val outputDirectory: String = config.getString("datapipeline.outputDirectory")

  // Data schema file path
  val dataSchemaFilePath: String = config.getString("datapipeline.dataSchema")

  // Data schema
  val dataSchema: StructType = loadDataSchema(dataSchemaFilePath)

  // Spark configuration
  val sparkConfig: SparkConfig = {
    val sparkConfig = config.getConfig("spark")
    SparkConfig(
      master = sparkConfig.getString("master"),
      appName = sparkConfig.getString("appName")
      // Add more Spark configurations as needed
    )
  }

  // File paths as Path objects

  // Convert input file paths to Path objects
  val inputFilePaths: List[Path] = inputFiles.map(filePath => Paths.get(filePath))

  // Convert output directory path to Path object
  val outputDirectoryPath: Path = Paths.get(outputDirectory)

  // Load data with specified schema
  def loadData(inputPath: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .schema(dataSchema)
      .load(inputPath)
  }

  // Load data schema from file
  private def loadDataSchema(schemaFilePath: String): StructType = {
    val schemaConfig = ConfigFactory.parseFile(new java.io.File(schemaFilePath))
    val fields = schemaConfig.getConfigList("columns").asScala.toList.map { columnConfig =>
      val name = columnConfig.getString("name")
      val dataType = columnConfig.getString("data_type")
      StructField(name, convertToSparkType(dataType))
    }
    StructType(fields)
  }

  def loadData(inputFilePaths: List[Path], dataSchema: StructType): DataFrame = {
    // Use Spark session to load data
    val rawDF = spark.read.schema(dataSchema).csv(inputFilePaths.map(_.toString): _*)
    rawDF
  }

  // Map your data type to Spark data type
  private def convertToSparkType(dataType: String): DataType = {
    dataType.toLowerCase match {
      case "string" => org.apache.spark.sql.types.StringType
      case "integer" => org.apache.spark.sql.types.IntegerType
      case "double" => org.apache.spark.sql.types.DoubleType
      case "timestamp" => TimestampType
      // Add more cases as needed based on your data types
      case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType")
    }
  }
}


case class SparkConfig(master: String, appName: String)
