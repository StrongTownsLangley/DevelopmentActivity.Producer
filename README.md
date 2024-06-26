# DevelopmentActivity Project
Strong Towns Langley's DevelopmentActivity project seeks to use real-time data processing tools such as Kafka and the ELK Stack to track and analyze the Development Activity in the Township of Langley, allowing the tracking of new developments, the rate of new developments and applications, approval times, and so on, which will be derived from the static data snapshots available on the Township of Langley's open data portal.

In particular this will be used to track the uptake and success of the pending local implementation of [British Columbia's Bill 44 **SSMUH** Multiplex legislation](https://www2.gov.bc.ca/gov/content/housing-tenancy/local-governments-and-housing/housing-initiatives/smale-scale-multi-unit-housing) as data is being tracked beginning in April 2024, with implementation scheduled for June 2024.

## DevelopmentActivity.Producer Module
This module is written in C# .NET 6.0 and uses the Confluent.Kafka library to send Development Activity data to a Kafka instance and the Elastic.Clients.Elasticsearch library to send data to an ElasticSearch instance. It downloads the latest Development Activity data in JSON format, compares it with the last data in the Kafka topic and/or ElasticSearch index, and if it has changed, updates them with the latest data.

![image](https://github.com/StrongTownsLangley/DevelopmentActivity.Producer/assets/160652425/f181bce6-9107-4d53-9cb0-a488e766086a)

## How to install on Linux (Ubuntu)
1. Install the [.NET 6.0 runtime for linux]([https://learn.microsoft.com/en-us/dotnet/core/install/linux-ubuntu-install?pivots=os-linux-ubuntu-2004&tabs=dotnet6)
2. Upload the source code to a folder on the server and cd to the folder
3. Run **dotnet build**
4. Place **config.xml** in binary folder (bin/debug/net6.0)
5. Run **dotnet run** (you may wish to do this in [screen](https://www.gnu.org/software/screen/manual/screen.html) to allow disconnecting/reconnecting)   

This project is written by James Hansen.

## Data Source
The Township of Langley releases this data with no license requirements or restrictions on their [OpenData Portal](https://data-tol.opendata.arcgis.com/). They also provide an API to read the data in JSON format as detailed on the [About Page for the Development Activity Status Table](https://data-tol.opendata.arcgis.com/datasets/TOL::development-activity-status-table/about). 

Currently this API URL is
https://services5.arcgis.com/frpHL0Fv8koQRVWY/arcgis/rest/services/Development_Activity_Status_Table/FeatureServer/1/query?outFields=*&where=1%3D1&f=json
The "f" parameter is shown on the about page as *geojson*, but as their is no geojson data such as polygon information, we can change this to *json* and also receive all the column specifications as well as the data.

## Status
The producer is complete and operational, running on our StrongTownsLangley.org VPS server alongside the Apache Kafka and ElasticSearch / Kibana instance.

Demo access to the Kibana for live viewing of the data coming soon.
