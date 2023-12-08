# DataPipeline on Adtech-related data


## Project Description

This project implements a data processing 
pipeline using Apache Spark and Scala. The pipeline includes data cleaning, 
processing, and visualization components.

it will generate three csv files for result based on 

1. Aggregate total impressions for each site_id and ad_type_id.
2. Calculate average revenue for each advertiser_id
3. Revenue Share by Monetization Channel: Analyze revenue share across different monetization_channel_ids.

## Author

- **Author Name**: [Leo Hu]


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

```
## Requirements
Apache Spark 3.x
Scala 3.x
sbt (Scala Build Tool)


## Instructions for Running the Project
Clone the repository:
Copy code
```bash
git clone https://github.com/qingchuh/Adtech_Datapipeline.git
cd MagniteTakeHome
```


##configuration files
```plaintext
project-root/
│
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── application.conf
│   │   │   ├── data_schema.json
│   │   │   ├── Dataset.csv
```

Build the project:

```bash
sbt clean compile
```
Run the data processing pipeline:
```bash
sbt run
```

### Warnings and Errors
there might be some Warnings and errors on dependencies, please ignore them 
if you see success and output file is generated


## Output 
output files will be generated in 
and console will also show first 10 rows on result dataframes
and total time cost to run the proj like:
Total time spent: 45.545 seconds

```plaintext
project-root/
│
├── outptut/
│   ├── aggregated_data.csv/ *.csv
│   ├── average_revenue_data.csv/ *.csv
│   ├── revenue_share_data.csv/ *.csv


```

## Challenges and Solutions

###Challenges: 
1. Performance issue due to large data file
2. Development scalability due to further expansion on the datapipeline
3. Testing Spark applications can be complex because you can only test small amout of data

###Solutions: 
1. Allow more spark optimization inside dataprocessing methods, 
create more parallelization, 
optimizing aggregation method such as percentile
Utilize Spark's distributed computing capabilities 
and consider partitioning data to distribute the workload.

2. Design pipeline to be horizontally scalable.include more 
   dependency injection properties, allowing swithing between local development
   and testing development more freely, put properties in a central management 
   place
   
3. Implement unit tests for individual components using tools like Spark Testing Base. 
   Use mock testing for Spark context and other dependencies. 
   Conduct integration tests to validate the entire pipeline.


###Additional Notes
The project uses Apache Spark for distributed data processing.
For additional visualizations, extend methods in Visualization.scala.
Contributions and feedback are welcome!
