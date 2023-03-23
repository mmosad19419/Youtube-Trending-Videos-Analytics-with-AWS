# Youtube Trending video Analytics With AWS 
## Overview
This project aims to securely manage, streamline, and analyze the __structured__ and __semi-structured__ YouTube video data based on the video categories and trending metrics.

Using __AWS cloud Computing__ platform to build an End-to-End data pipeline for processing and storage, connect with __Microsoft PowerBi__ as BI Tools to build the reports.

## Dataset Trending YouTube Video Statistics
This Kaggle dataset contains statistics (CSV files) on popular daily YouTube videos over the course of many months. There are up to 200 trending videos published every day for many locations. The data for each region is in its own file. The video title, channel title, publication time, tags, views, likes and dislikes, description, and comment count are among the items included in the data. A category_id field, which differs by area, is also included in the JSON file linked to the region.

Find more about the dataset [Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new)


## Tools, and Services used
1. __Amazon S3__: Amazon S3 is an object storage service that provides manufacturing scalability, data availability, security, and performance.
  * It is used as the staging layer for our raw data
  * Data Lake for raw, cleansed, and analytical data files

2. __AWS Glue__: A serverless data integration service that makes it easy to discover, prepare, and combine data for analytics
  * Used as the Data integration tool for our project.
  * Discover and build our raw, cleansed, and analytical metadata.
  * Create a Catalog for our data lake so we can easily query our data using AWS Athena.
  
3. __AWS Athena__: Athena is an interactive query service for S3 in which there is no need to load data it stays in S3.
  * Query our data from the data lake
  * Put a structure over our data lake to query our data easily and in an efficient way.

4. __AWS Lambda__: Lambda is a computing service that allows programmers to run code without creating or managing servers.
  * Extract, Transform our data files after they land in our raw data area.
  * Make the ETL process automated using lambda triggers.
  * Configure it to load the data from the raw area, clean it, and move it to the cleansed data area in our data lake.

5. __Microsoft PowerBi__: Serve as our BI tool to build our reports.
  * Connect to AWS Athena to load the data from our analytical storage area to build our reports.
  * Provide a secure connection, and automated reporting to enhance the decision-making process.
  
6. __AWS IAM__: Identity and access management which enables us to manage access to AWS services and resources securely.
  * Define different roles for different users.
  * Allow managing data governance and Access management of different layers in our AWS S3 storage.
  
## Data Infrastructure and Architecture
![architecture](https://user-images.githubusercontent.com/80867381/227107808-6646241e-7a52-423a-af7f-3ac72569dbb0.jpeg)

### Source Systems and Data Acquisition
Build a bash script to get the data through the __Kaggle API__ and Push it to the __AWS S3__ data lake raw area storage using __AWS CLI__.
__file__ `dataTransfer.sh`

### Datalake Architecture
##### Naming Conventions
Project-based/[raw, curated]/region/environment type [dev, production]/data partitioning < source/type/ region>
  `ex: data_engineering_youtube_analytics/raw/us-east-1/dev/youtube/raw_statistics/region=ca/`
  
##### Layers
1. Raw Area 
  * Staging area to store the data coming from different sources in raw format.
2. Cleansed Area
  * Store the cleansed Data after processing using AWS Lambda.
3. Analytics Area (Curated Area)
  * Store-ready analytics data to be queried directly using Athena and Exported to Microsoft Power Bi.

![youtubeAnalytics9](https://user-images.githubusercontent.com/80867381/227109824-856149c3-15af-4ea8-8abc-53ead4e505ae.JPG)


### Data Processing Layer
1. Process the data on arrival to the raw data area using **AWS Lambda**
  *Using AWS Lambda function to build automated data cleaning process adding trigger to call the function
  whenever any new file arrive in the raw area and redirect it to the next layer

  __lambda function file__ `youtube_analytics_json_to_parquet.py`
  
2. Build a Data Catalog above the cleansed Data to query it using Athena
  * Using __AWS GLUE__ To build a structure layer over the cleansed data and store it in the cleansed database to query
  using Athena.
  
  * Build a pyspark job to 
      1. Drop Nulls
      2. Change Data types
      3. Filter data based on the region
      4. Schema Mapping
      5. Save the output to the cleansed database
   
  __file__ `youtube_analytics_cleansed_etl_csv_to_parquet.py`
      
![youtubeAnalytics3](https://user-images.githubusercontent.com/80867381/227110079-a432c11c-cc6b-4c85-9b5a-6732b2115d6c.JPG)
![youtubeAnalytics2](https://user-images.githubusercontent.com/80867381/227110077-1d514939-3848-4f61-ac6b-ad0d5316ba0d.JPG)

3. Join Different Tables to Build the analytical Table
  * Build a pyspark job to build the analytical table and load it to the analytical database 
    1. Join Tables
    2. Apply schema mapping
    3. Build analytical data and load it into an analytical database
    
  __file__ `etl_final_analytics_table.py`
  
![youtubeAnalytics5](https://user-images.githubusercontent.com/80867381/227112518-f7a2b4ae-0149-48b7-9044-56cdc1f4d7a0.JPG)
![youtubeAnalytics8](https://user-images.githubusercontent.com/80867381/227112527-f80e0d73-ba14-4555-bb1f-0ca186e515f6.JPG)

### BI Analytics and Reporting layer
1. Query the data Explore, and Export it to Microsoft PowerBI using __AWS Athena__
   __file__ `athena_sql_queries.sql`
   
2. Build Initial Report Using Microsoft PowerBi
  * Summarization of Trending Content Categories based on each country
  * Views Analytics
  * Likes, Dislikes, and Comments Analytics
  
![youtubeAnalytics10](https://user-images.githubusercontent.com/80867381/227114141-917533ac-9907-4209-bcce-4c9126899484.JPG)
