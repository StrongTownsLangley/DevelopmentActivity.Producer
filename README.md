# DevelopmentActivity Project
Strong Towns Langley's DevelopmentActivity project seeks to use the Apache real-time data processing stack (Kafka, Flink, etc) to track the Development Activity in the Township of Langley, allowing the tracking of new developments, the rate of new developments and applications, approval times, and so on, which will be derived from the static data snapshots available on the Township of Langley's open data portal.

## Data Source
The Township of Langley releases this data with no license requirements or restrictions on their [OpenData Portal](https://data-tol.opendata.arcgis.com/). They also provide an API to read the data in JSON format as detailed on the [About Page for the Development Activity Status Table](https://data-tol.opendata.arcgis.com/datasets/TOL::development-activity-status-table/about). 

Currently this API URL is
https://services5.arcgis.com/frpHL0Fv8koQRVWY/arcgis/rest/services/Development_Activity_Status_Table/FeatureServer/1/query?outFields=*&where=1%3D1&f=json
The "f" parameter is shown on the about page as *geojson*, but as their is no geojson data such as polygon information, we can change this to *json* and also receive all the column specifications as well as the data.

## Status
The producer is complete and operational, running on our StrongTownsLangley.org VPS server alongside the Apache Kafka instance.

## About
This module is written in C# .NET 6.0 and uses the Confluent.Kafka library. This project is written by James Hansen.
