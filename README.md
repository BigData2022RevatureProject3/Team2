# Team2
## Project Description
In this document, all rules, and requirements on how to proceed with defining and 
implementing the Revature capstone project are defined.
In this project the Cohort will be divided into two teams. Each team will begin by creating a data 
producer that will generate real time data simulating orders from an E-Commerce application.
Each team will then consume the output data from the other team through Kafka and run additional 
processing through Spark.
The final goal will be to decipher the algorithms used to generate data from the other team based on 
the output.

## Tasks
1. Create a producer program that will ingest data to a Kafka Topic.
  - Data will have to be generated in the program.
  - Up to 5% of the data can be bad data
  - The data generation methods must be self-sustaining
    - No changing the algorithm unannounced during the project
    - No hard coded value changes based on dates ie. X% increase on 11/30/2021
    - Ingest the data from the other team every 2 seconds into the Kafka Topic.
2. Display the data from the input Kafka Topic in a console consumer (CLI).
3. Create a consumer program in Spark that will read and clean the data stream from the input 
Kafka Topic and will process the data further.
  - Read the data into Data Frame objects.
  - Print the schema of the input data stream
  - Apply the above-mentioned schema to the data frames and print the schema.
  - Apply Exception Handling wherever applicable for a stable application.
  - From the consumer program:
    - Collect the data and manipulate/aggregate it to best allow you to predict what logic is being used to produce the data.
  - Display a visualization of all above outputs in Zeppelin.
