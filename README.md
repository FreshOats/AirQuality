# SmokeScreen - Wildfire Air Quality Monitor
**Air quality monitoring and equitable solutions for populations peripheral to wildfires**

### *by* Justin R. Papreck

## Executive Summary
As the occurrences of wildfires continues to increase with changing climate, we continue to see the direct impact on devastated forests, homes, and neighborhoods. What is often ignored is the indirect impact on people who are not directly affected, but rather tangentially affected by chronic exposure to airborne particles or pollutants in their water as a result of the fires themselves or the materials used to suppress the fires. 

What this project intends to do is observe the regions surroudinging the wildfires to assess air quality. Using sensor measurements, different contaminants are assessed by severity, and with these severity levels, different solutions can be recommended. To provide the best recommendations for that region, census data considers the median income level of census tracts located near the different sensors, as well as the liklihood of accurate prediction of the reading. This information is to serve two purposes: 

1. To inform the affected populations of their exposure, health risks, and appropriate potential solutions
2. Provide this information to vendors of the protective equipment such as masks, filters, or filtration systems. 

My hope is that by stratifying income levels, companies can provide discounted materials to lower income tracts for populations that may not be able to afford the adequate protective products they need, whereas higher income tracts can make up for any losses with purchases of higher-end or top-of-line products. This creates a win-win for the clients and the vendors by bringing positive attention to vendor participants in addressing the health concerns equitably, as well as helping the clients at least get the minimum protective material for their families in regions that will not receive any federal or state aid, as they are not directly affected by the actual fires. 

### Data Sources
Wildfire data is collected from the NASA FIRMS API, which is updated in Near-Real Time frequency. 
Census data is collected from the US Census, TIGERS/ACS data, which provides the geometric region, centroid coordinate, median income and margins for each census tract. 
Air Quality data is collected from OpenAQ, an open source Air Quality database that updates every 15 minutes. 

### Pipeline Architecture
The overall architechture follows the medallion structure with a Static Layer in addition to the Bronze, Silver, and Gold Layers. 
1. Static Layer - Broken up in to 8 files. This collects the census data and some backfill data from OpenAQ and FIRMS. This is code that was only run once to set up reference tables that would require expensive processing if called each time. 
2. Bronze Layer - Raw Data Acquisition from the FIRMS database and the OpenAQ database as the extract and load parts of ELT. This validates the data and merges into the permanent raw storage with minimal steps to ensure idempotency. There are separate Bronze Layer notebooks for the FIRMS and the OpenAQ data, as they are part of a feedback loop that requires the output for the FIRMS silver as the input to collect OpenAQ raw data in Bronze.
3. Silver Layer - The bulk of the transformations, filtering, consolitdation, and joining different tables. Like the Bronze layers, there are two silver layer files for the processing of the FIRMS to refine collection of the AQ data. The silver layer for the AQ data prepares all of the processed and joined delta tables that referenced in the Gold Layer
4. Gold Layer - Final aggregations that will be used for visualizations.

### Technology Used
Data Ingestion and Processing is all being handled in Databricks. The output tables from the Gold Layer are pulled directly from Databricks into Power BI to create visualizations and a deployable dashboard. LucidChart was used to establish the workflow, and orchestration is performed in Databricks Workflows. 

** The original plan included using external AWS S3 storage with iceberg and dbt to run tests on the data, such as unit tests. Additionally, I planned to pull data from the Silver Layer into Snowflake for easy querying and connectivity to Power BI. After some consideration, I chose to drop the use of Snowflake, as this incurs additional storage and compute fees in addition to those incurred by Databricks, and the same processes can be done seamlessly in Databricks. Regarding the tests, these have yet to be implemented, and I haven't decided whether I will be using Alerts within Databricks or dbt. Keeping the system simple often correlates with more robust architecture, which is why at the moment, I am keeping everything from ingestion through the Gold Layer Processing in Databricks. The decision to use Power BI for analytics rather than Databricks is for external access and active visualization outside of a Databricks account. 

---
## Static Layer Processes

1. Static Layer 1 - TIGER Geodata Census Ingestion
2. Static Layer 2 - Sensor Location Processing
3. Static Layer 3 - Geo Census Validation and Filter
4. Static Layer 4 - Nearest Neighbor Analysis
5. Static Layer 5 - Sensor Census Merge
6. Static Layer 7 - FIRMS Backfill
7. Static Layer 8 - Air Quality Parameter References

The Medallion Architecture usually starts with the Bronze Layer for raw data ingestion, but some of the data ingested here is large, and it requires heavy processing that only needs to be done once, to set up reference tables that will not change over time - at least no until the next US Census or addition of new Air Quality Sensors. These are a bottom priority for the current pipeline, but could be added at a later time. 

1. *TIGER Geodata Census Ingestion* The data acquired for the US Census was found from the TIGER/Line Shapefiles - Census.gov website. The granularity I wanted to find was that of a **census tract**. A census tract is smaller than a zip code, and is generally sized to include between 4000 and 5000 residents. The higher order of granularity is the zip code and county, which can become massive geometrically, which is not ideal for this project. Going into more granular detail are the block groups and subsequently blocks. While these do have the best information demographically, there is the least up-to-date information about them, and the processing for them becomes exponentially larger, when identifying the nearest sensors. 

The ACS - American Community Survey provides a 5-year estimate with demographic data with granularity to the tract level, and also includes the shapefile, boundaries, and centroid coordinates for the census tracts. To acquire this data, I downloaded this data locally, and then uploaded it to Databricks in the compressed files for each of the 50 states, Washington DC, and additional territories. 

The file, TIGERS Geodata Census Ingestion unzips each of the census directories, each containing 3045 codified columns, with rows for each census tract. The fucntion *process_gdb* takes the file path, extracts only the columns necessary from the spatial and attribute layers, and renames them to appropriate column headers. This is iterated through for each state file and appends the data from all states to a single Parquet file, Geo_Census.parquet in the local Databricks Volumes. 

2. *Sensor Location Processing* collected data from all sensors in the United States from OpenAQ's archival data, stored in an AWS S3 bucket, and saved each of these locations in the us_all_locations_data.csv file. The data collected from the API was collected from JSON form, so some columns contained nested data. This was important, since each location has a different number of sensors, and each row from this set is per location, NOT per sensor - reducing the size of the data stored and the complexity of joins in future data transactions. This raw data was stored as a csv, us_all_locations.csv 

The location data was validated, ensuring that all sensor coordinates are within the latitude/longitude boundaries of the US, checking for duplicate entries, and null values. There were sensors where datetime_last (the last time the sensor collected a reading) were null, and these were removed from the dataset. Additionally, any sensors that have not been active since Jan 1, 2025 were also removed, since I only want to check current sensors, and also don't want to assign tracts to sensors that don't collect air quality data.

After these filters were performed, the data table was expanded so that each row was per sensor, expanding for any locations that contain multiple sensors, and saved as Delta Table, filtered_sensors

3. *Geo Census Validation and Filter* removes all territories other than Washington, DC, and maintains all of the states, including Hawaii and Alaska. During validation, only one location failed, and that happened to be a single census tract on the international dateline, and doesn't fall within the general boundary of the US. The raw data contains a lot of information, but for the purpose of this application, I only need the primary key, centroid coordinates for each tract, the state, median income and margins of error. This was saved to filtered_geo_census.parquet

4. *Nearest Neighbor Analysis* is a heavy process that goes through each of the census tracts performing a cross join with the sensor locations, performs a cartesian distance calculation, and then ranks each tract ordered by ascending distance, collected the lowest distance as the nearest neighbor. Due to the size of these data tables, I had to increase the parameters for processing in Databricks, as noted in the file. This processing was performed with Spark Sql and saved as a Delta Table

5. *Sensor Census Merge* merges the Delta table nearest_neighbors with filtered_sensors, adng filtered_geo_census, resulting in a table that is at the granularity of (Census_Tract, Sensor_id). Additional parameters were added to classify income stratification and confidence of the air quality reading based on the distance from the centroid of the census tract. This was saved as a Delta Table sensors_with_income_levels

6. *Backfill* collects data from the OpenAQ AWS storage for a fixed number of dates. The current code only backfills the Los Angeles region, which was the test area for the code. This was used primarily for testing the working code to maintain idempotency when filling in new data, especially with non-scheduled API calls potentially duplicating data - this was employed in the Bronze Layer. 

7. *FIRMS Backfill* collects backfill data from the beginning of February 2025 from the NASA FIRMS API. Again, this was primarily used to validate idempotent processes for the bronze layer, and also determine timing for the number of days in the past I need to collect data for from the OpenAQ API, while balancing the limits of the API calls. 

8. *Air Quality Reference Table* is exactly what is sounds like. The most recent data collected by location from OpenAQ only contains the location_id, sensor_id, and value. This reference table gives the parameter name and units associated with the value collected by that sensor. Additionally, it contains safety limits for each parameter, noting if it is Moderate (not safe), Severe, or Hazardous. These are each their own column, which is used in assessing the collected data.  