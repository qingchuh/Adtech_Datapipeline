# DataPipeline on Adtech-related data


## Project Description

This project implements a data processing pipeline using Apache Spark and Scala. The pipeline includes data cleaning, processing, and visualization components.

## Project Structure

```plaintext
project-root/
│
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   ├── cleaning/
│   │   │   │   ├── DataCleaner.scala
│   │   │   ├── configuration/
│   │   │   │   ├── ConfigReader.scala
│   │   │   ├── processing/
│   │   │   │   ├── DataProcessor.scala
│   │   │   ├── visualize/
│   │   │   │   ├── Visualization.scala
│   │   │   ├── datapipeline.scala
│   ├── test/
│   │   ├── scala/
│   │   │   ├── cleaning/
│   │   │   │   ├── DataCleanerTest.scala
│   │   │   ├── configuration/
│   │   │   │   ├── ConfigReaderTest.scala
│   │   │   ├── processing/
│   │   │   │   ├── DataProcessorTest.scala
│   │   │   ├── visualize/
│   │   │   │   ├── VisualizationTest.scala

Requirements
Apache Spark 3.x
Scala 3.x
sbt (Scala Build Tool)


Instructions for Running the Project
Clone the repository:

bash
Copy code

git clone https://github.com/qingchuh/Adtech_Datapipeline.git
cd MagniteTakeHome



configuration file is in application.conf and data_schema.json.

data file is in 

Build the project:

bash
Copy code
sbt clean compile
Run the data processing pipeline:

bash
Copy code
sbt "runMain datapipeline.scala"

Challenges and Solutions
Challenge 1: Spark Configuration
Solution: Ensure correct Spark configurations in application.conf and ConfigReader.scala. Test configurations with ConfigReaderTest.scala.

Challenge 2: Data Cleaning
Solution: Implement data cleaning methods in DataCleaner.scala. Write tests in DataCleanerTest.scala to ensure proper functionality.

Additional Notes
The project uses Apache Spark for distributed data processing.
For additional visualizations, extend methods in Visualization.scala.
Contributions and feedback are welcome!
