#! /bin/bash

# install project dependencies
pip install -r requirements.txt

# setup Kaggle API and run this command to get the youtube dataset
kaggle datasets download -d datasnaek/youtube-new

# mv data to data dir and un zip
mkdir ./data
mv youtube-new.zip ./data
unzip ./data/youtube-new.zip -d ./data
rm ./data/youtube-new.zip


# create s3 bucket with the naming conventions and move the data to it
aws s3 cp ./data s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics_reference_data/ --recursive --exclude "*" --include "*.json" 

# To copy all data files to its own location, following Hive-style patterns:
aws s3 cp ./data/CAvideos.csv s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics/region=ca/
aws s3 cp ./data/DEvideos.csv s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics/region=de/
aws s3 cp ./data/FRvideos.csv s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics/region=fr/
aws s3 cp ./data/GBvideos.csv s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics/region=gb/
aws s3 cp ./data/INvideos.csv s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics/region=in/
aws s3 cp ./data/JPvideos.csv s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics/region=jp/
aws s3 cp ./data/KRvideos.csv s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics/region=kr/
aws s3 cp ./data/MXvideos.csv s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics/region=mx/
aws s3 cp ./data/RUvideos.csv s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics/region=ru/
aws s3 cp ./data/USvideos.csv s3://youtube-analytics-raw-us-east-1-dev/youtube/raw_statistics/region=us/

# remove all file loclly
rm -rf ./data/*.csv
rm -rf ./data/*.json
rm -rf ./data

