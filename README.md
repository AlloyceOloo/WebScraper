# Rental Properties WebScraper
This is a Web scraper for rental properties on Property24.com/kenya

## Setup
* Install Python3 and pip3
* Install Numpy, Pandas, lxml and Requests
* Install bs4, json and googlemaps

## Data Format in CSV file
Each row represents a single rental property.
The header is not included but it is structured as follows:

* Property Title | Property Rental Price (KSH) | Property Location | Property Bedrooms | Property Bathrooms | Distance From the CBD (haversine)

Apache Kafka used as Streaming Platform

MongoDb used as Data Lake

