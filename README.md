# SmokeScreen - Wildfire Air Quality Monitor
**Air quality monitoring and equitable solutions for populations peripheral to wildfires**

### *by* Justin R. Papreck

## Executive Summary
As the occurrences of wildfires continues to increase with changing climate, we continue to see the direct impact on devastated forests, homes, and neighborhoods. What is often ignored is the indirect impact on people who are not directly affected, but rather tangentially affected by chronic exposure to airborne particles or pollutants in their water as a result of the fires themselves or the materials used to suppress the fires. 

What this project intends to do is observe the regions surroudinging the wildfires to assess air quality. Using sensor measurements, different contaminants are assessed by severity, and with these severity levels, different solutions can be recommended. To provide the best recommendations for that region, census data considers the median income level of census tracts located near the different sensors, as well as the liklihood of accurate prediction of the reading. This information is to serve two purposes: 

1. To inform the affected populations of their exposure, health risks, and appropriate potential solutions
2. Provide this information to vendors of the protective equipment such as masks, filters, or filtration systems. 

My hope is that by stratifying income levels, companies can provide discounted materials to lower income tracts for populations that may not be able to afford the adequate protective products they need, whereas higher income tracts can make up for any losses with purchases of higher-end or top-of-line products. This creates a win-win for the clients and the vendors by bringing positive attention to vendor participants in addressing the health concerns equitably, as well as helping the clients at least get the minimum protective material for their families in regions that will not receive any federal or state aid, as they are not directly affected by the actual fires. 

## Data Sources
Wildfire data is collected from the NASA FIRMS API, which is updated in Near-Real Time frequency. 
Census data is collected from the US Census, TIGERS/ACS data, which provides the geometric region, centroid coordinate, median income and margins for each census tract. 
Air Quality data is collected from OpenAQ, an open source Air Quality database that updates every 15 minutes. 

## Pipeline Architecture
The overall architechture follows the medallion structure with a Static Layer in addition to the Bronze, Silver, and Gold Layers. 
1. Static Layer - Broken up in to 8 files. This collects the census data and some backfill data from OpenAQ and FIRMS. This is code that was only run once to set up reference tables that would require expensive processing if called each time. 
2. Bronze Layer - Raw Data Acquisition from the FIRMS database and the OpenAQ database as the extract and load parts of ELT. This validates the data and merges into the permanent raw storage with minimal steps to ensure idempotency. There are separate Bronze Layer notebooks for the FIRMS and the OpenAQ data, as they are part of a feedback loop that requires the output for the FIRMS silver as the input to collect OpenAQ raw data in Bronze.
3. Silver Layer - The bulk of the transformations, filtering, consolitdation, and joining different tables. Like the Bronze layers, there are two silver layer files for the processing of the FIRMS to refine collection of the AQ data. The silver layer for the AQ data prepares all of the processed and joined delta tables that referenced in the Gold Layer
4. Gold Layer - Final aggregations that will be used for visualizations.

## Technology Used
Data Ingestion and Processing is all being handled in Databricks. The output tables from the Gold Layer are pulled directly from Databricks into Power BI to create visualizations and a deployable dashboard. LucidChart was used to establish the workflow, and orchestration is performed in Databricks Workflows. 