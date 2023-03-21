-- Create Databases
CREATE DATABASE youtube-analytics-cleansed;

CREATE DATABASE youtube-analytics-raw;

CREATE DATABASE youtube-analytics;

-- Explore our Raw and cleansed tables
SELECT * FROM "AwsDataCatalog"."youtube-analytics-cleansed"."raw_statistics_cleansed" limit 5;

SELECT * FROM "AwsDataCatalog"."youtube-analytics-cleansed"."raw_statistics_cleansed_reference_data" limit 5;

-- Join the two table 
SELECT * FROM "AwsDataCatalog"."youtube-analytics-cleansed"."raw_statistics_cleansed" r
INNER JOIN "AwsDataCatalog"."youtube-analytics-cleansed"."raw_statistics_cleansed_reference_data" rr
ON r.category_id = rr.id
limit 5;

-- Final Analytical Table
SELECT * FROM "AwsDataCatalog"."youtube-analytics"."youtube_analytics_table" limit 10;


-- Create Report View
CREATE OR REPLACE VIEW powerBiView AS
SELECT *
FROM youtube_analytics_table;