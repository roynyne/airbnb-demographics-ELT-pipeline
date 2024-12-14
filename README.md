# airbnb-demographics-ELT-pipeline

A scalable ELT pipeline for analyzing Airbnb listings and demographic data using Apache Airflow, dbt Cloud, Google Cloud Storage, and PostgreSQL. The pipeline follows the Medallion Architecture (Bronze, Silver, Gold) to ingest, clean, transform, and organize data for insightful analysis and reporting.


## 1. Project Overview
The goal of this project is to create a reliable, automated data pipeline that can ingest, convert, and analyse census and Airbnb statistics to provide insightful information about neighbourhood demographics and Airbnb listing performance. This pipeline uses Dbeaver-PostgreSQL, Apache Airflow, dbt Cloud, and Google Cloud Storage (GCS) in conjunction with the Medallion Architecture (Bronze, Silver, Gold) to organise data for the best possible analysis.
The Medallion Architecture layers are designed as follows:
Bronze Layer: Retains unprocessed raw data from a variety of sources, including local government entities, Airbnb, and census data.
Silver Layer: Prepares organised tables for analysis by cleaning and standardising the data.
Gold Layer: Uses certain datamart views to address business problems and arranges data into a star schema for effective querying.
Every stage of the project is described in this report, including data ingestion, transformation, Airflow orchestration, and analytical insights from dbt-generated views.

## 2. Dataset Overview
Both static and dynamic data were utilised in this project; they were all kept in Google Cloud Storage and accessible via Airflow for PostgreSQL ingestion.
Airbnb Listings: A set of monthly CSV files with listing data, including listing_id, host_name, property_type, price, and availability metrics (e.g., 05_2020.csv to 04_2021.csv). To ensure chronological consistency, this data is fed into the pipeline month by month.
Census Data: 2016Census_G01_NSW_LGA.csv and 2016Census_G02_NSW_LGA.csv primary census files include socioeconomic and demographic data on the Local Government Areas (LGAs) of New South Wales. While G02 covers measures like median age, income, and rent, G01 contains population statistics.
LGA Codes and Suburb Mapping: Suburbs are mapped to LGAs using two more static datasets, NSW_LGA_CODE.csv and NSW_LGA_SUBURB.csv. Analysis of listings at the neighbourhood level is supported by this spatial reference.
By storing these datasets in GCS, scalability is guaranteed in the event that additional files are added or dataset sizes grow, and flexible access for ingestion is made possible.

## 3. Data Ingestion and Staging (Bronze Layer)
In order to ensure data integrity and facilitate traceability, the Bronze layer acts as the fundamental staging area for raw data intake, maintaining each dataset's original structure. With just few modifications, all datasets are fed into PostgreSQL tables inside the bronze schema.

### a. Airflow DAG Setup for Data Ingestion
The data intake procedure is coordinated by a load_to_bronze_schema_sequential Apache Airflow Directed Acyclic Graph (DAG). In order to guarantee that data from all sources is present in the Bronze layer prior to downstream operations starting, the DAG comprises jobs for loading static files first, then monthly Airbnb datasets.

The Airflow DAG consists of:

Static File Ingestion: Static files (census data and LGA mappings) are downloaded and loaded from GCS to PostgreSQL in separate operations. Each job loads data into PostgreSQL using a PostgresHook and downloads files using the GCSToLocalFilesystemOperator and a custom load_to_postgres function.
Sequential Monthly Airbnb Ingestion: The DAG loads each Airbnb file one after the other in order to preserve chronological order. A loop that creates a task for every month and uses dependencies between jobs to guarantee the right sequence enforces this order.

![3](https://github.com/user-attachments/assets/6d59b45f-f439-426e-a199-c16b91d468d9)
Figure 3.1 Google Cloud Services (GCS) bucket

![4](https://github.com/user-attachments/assets/064c98ff-066d-48fc-bcae-3d1aae9431b6)
Figure 3.2 DBeaver Postgres Bronze Schema

![5](https://github.com/user-attachments/assets/b2915284-3fc5-4d38-999a-1055f30dbbb1)
Figure 3.3 All months imported from Airflow

### b. Schema and Table Design in the Bronze Layer
Tables in the Bronze schema reflect each CSV file's structure. For instance, Airbnb data with attributes like listing_id, host_name, and price is stored in raw_airbnb_listings. This table acts as a staging region, keeping any raw fields that could require cleaning or restructuring in the following levels while storing data precisely as it appears in the source.

Other tables in the Bronze schema include:

raw_census_g01 and raw_census_g02: These tables, which include variables like total_population_male, median_age_persons, and median_rent_weekly, hold demographic and socioeconomic data.
raw_lga_codes: Includes LGA names and codes, making it possible to identify LGAs.
raw_lga_suburbs: helps with neighborhood-level aggregations later on by mapping suburbs to their corresponding LGAs.

### c. Loading Process and Data Quality Control
The load_to_postgres function manages row-wise insertion into PostgreSQL and eliminates unnecessary columns (such as unnamed columns from CSV outputs) to guarantee data quality. By allowing for precise control over the integrity of each row, this procedure guarantees that only structured data makes it to the Bronze tables.

![6](https://github.com/user-attachments/assets/2f07d50a-d38e-47f7-a042-75586eb7a191)
Figure 3.4 Airflow DAG (load_to_postgres)

Airflow's logging features are used to keep an eye on job execution and debug any data ingestion problems, including missing files or corrupted rows, during the loading process.

## 4. Data Cleaning and Transformation (Silver Layer)
The Bronze layer's raw data is transformed into organised, analysis-ready tables by the Silver layer using data transformation and cleaning procedures. This layer makes ensuring that fields are correct, consistent, and structured for effective aggregation and joining in the Gold layer across many datasets.

![7](https://github.com/user-attachments/assets/482b2468-9312-45d6-a994-94f9bc8e4925)
Figure 4.1 dbtcloud folder structure

### a. Silver Layer Transformation Design

Every Silver layer model has a corresponding Bronze table and undergoes modifications that deal with:

Data Type Casting: For computations and aggregations, fields such as IDs and prices are converted to the proper kinds (such as integer or numeric).
Handling Missing Values: In fields like host_name and host_neighbourhood, null entries are swapped out for default values like "Unknown."
Standardizing Formats: Ensuring that dates and category categories are formatted consistently across tables, including standardising text fields' case and converting dates to YYYY-MM-DD.
Validating Values: Removing excessive or erroneous numbers, such as out-of-range review scores or negative pricing.

The dbt Cloud models created for this layer include:

s_airbnb_listings: This table filters and converts data types for Airbnb listings. For example, pricing is verified to be within a suitable range, and listing_id and host_id are cast to integers. Review ratings are verified to make sure they fall within a certain range, and columns such as host_is_superhost are transformed to Boolean for precise analysis.
s_census_g01 and s_census_g02: The process of cleaning census data involves converting population and socioeconomic indicators (such as total_population_male and median_rent_weekly) to integer or float types and eliminating non-numeric characters from lga_code_2016. To preserve data integrity, fields that are essential for joining, such as lga_code, must not be null.
s_lga_codes and s_lga_suburbs: By removing whitespace from names and making sure that all lga_code and suburb_name values are non-null, these tables clean and format data pertaining to LGAs. When combining with other tables for geographic analysis, this ensures consistency.

### b. Model Execution and Data Quality Checks
In DBT, every Silver layer model is set up to execute with data quality checks that confirm the existence and kind of important fields. Since transformations standardise the data and establish the groundwork for precise analysis in the Gold layer, data quality is essential in this tier. Validation checks on crucial fields (such as primary keys and foreign keys) guarantee that clean, structured data is accessible for additional processing while the models are run within dbt Cloud.

![8](https://github.com/user-attachments/assets/2f8dd398-8faa-454f-b282-bf6b6418b42b)
Figure 4.2 Lineage of airbnb_listings

![9](https://github.com/user-attachments/assets/95e52dd4-1c0a-4dd7-a1d8-bf40dd392db6)
Figure 4.3 Lineage of census_g01

![10](https://github.com/user-attachments/assets/0c05fe17-ffcc-4831-bf8d-bb7790dd3d4b)
Figure 4.4 Lineage of census_g02

![11](https://github.com/user-attachments/assets/9c539e96-1510-4d7d-adfc-eb55330f7204)
Figure 4.5 Lineage of lga_codes

![12](https://github.com/user-attachments/assets/49d30697-de54-4b8a-9f50-2002fd18af19)
Figure 4.6 Lineage of lga_suburbs


## 5. Snapshot Implementation

In dbt, snapshots are crucial for monitoring changes over time, especially for variables that could vary over time, such census demographics or listing pricing. Historical analysis and comparisons are made possible by this project's implementation of snapshots on Silver layer core tables.

![13](https://github.com/user-attachments/assets/eefa3b02-259b-4bc7-81e0-aaf52761a0b3)
Figure 5.1 dbtcloud snapshots folder

### a. Airflow DAG Setup for Data Ingestion

Strategy = 'check' is set up for each snapshot, comparing important fields (check columns) in every table to identify and document changes over time. Important snapshots consist of:
s_airbnb_listings_snapshot: Monthly changes in listing parameters such as price, availability_30, number_of_reviews, and review_scores_rating are captured in this snapshot. Each record is guaranteed to belong to a specific listing by the unique key listing_id, and any modifications made to the check fields cause a new version of that listing to be recorded.
s_census_g01_snapshot and s_census_g02_snapshot: Using distinct keys depending on lga_code, these snapshots document changes in the socioeconomic and demographic makeup of LGAs. To record any changes or new values over time, metrics like median_tot_hhd_inc_weekly and total_population_persons are monitored.
s_lga_codes_snapshot and s_lga_suburbs_snapshot: In order to preserve any revisions to LGA names or suburb mappings for future use, these snapshots save geographic data for LGAs and suburbs. When administrative borders or naming standards change, this is crucial for longitudinal data analysis.

### b. Benefits of Snapshotting
By preserving past data, snapshots enable in-depth trend analysis and comparisons across time. The project can address difficult business challenges by putting these pictures into practice, including:
Revenue and Price Trends: Tracking market dynamics is made easier by monthly variations in price and expected revenue across several listings.
Demographic Shifts: As census data changes, socioeconomic assessments of neighbourhoods are made possible, which helps inform judgements about Airbnb activities in certain demographic regions.

## 6. Star Schema Design (Gold Layer)
In order to facilitate analytics and optimise querying, the Gold layer arranges data into a star schema. This is especially useful for addressing business enquiries about demographic trends, revenue, and occupancy. With the help of dimension tables that offer descriptive qualities and fact tables that store metrics and foreign keys, this design facilitates easy aggregations and efficient joins.

![14](https://github.com/user-attachments/assets/b1576140-fbd2-4f27-b0b8-5b542e42b7ce)
Figure 6.1 dbtcloud gold folder structure

### a. Fact and Dimension Tables

The star schema in the Gold layer contains the following tables:

Fact Table: fact_listings:

Key metrics and connections to other dimensions are included in this table via foreign keys like listing_id and host_id. Price, number of reviews, availability_30, number of stays, and expected income are among the metrics kept in this table.
To enable effective analysis of monthly patterns, each row denotes a distinct listing for each month (month_year). For every listing, calculated data such as number_of_stays and estimated_revenue offer information on occupancy and revenue.

Dimension Tables:

dim_host: host_id, host_name, host_neighbourhood, host_since, and host_is_superhost are among the host properties that are stored. Analysis based on host quality and involvement (e.g., superhost status) is supported by this table.
dim_listing: Includes information on each listing, such as the listing's ID, neighbourhood, kind of property, type of room, and accommodations. This makes it possible to filter by listing attributes like neighbourhood and property type.
dim_neighbourhood: Allows for mappings between suburb_name and lga_name, facilitating multi-level geographic analysis.
dim_property_type: Includes room_type, accommodates, and property_type, which aid in classifying and evaluating postings according to the type of property.

### b. Schema Optimization and Data Integrity
This layout reduces data duplication and creates a normalised structure by using foreign keys to connect the fact table to its dimensions. Consistent joins and trustworthy analytics are made possible by data types and non-null constraints, which guarantee that important values are appropriately referenced across tables.
The pipeline can handle large-scale queries with little processing time when using this star schema. This schema is perfect for the datamart views in the following section since it streamlines aggregations, filtering, and joins.

## 7. Datamart Views
The Gold layer's datamart views offer organised insights designed to address certain business queries. Each view provides high-level metrics for decision-making by combining information from fact and dimension tables. To facilitate temporal analysis and represent Slowly Changing Dimensions Type 2, these perspectives are represented in dbt as monthly snapshots (SCD2).

### a. Dm_listing_neighbourhood

Data is compiled by listing neighbourhood and month in the dm_listing_neighbourhood view. Avg_estimated_revenue_per_active_listing, estimated_revenue, distinct_hosts, and percentage changes in active and inactive listings are among the metrics it offers.

Key metrics:
Total Listings and Active Listings: Monthly totals for all residential neighbourhoods' active and total listings.
Revenue and Host Engagement: Total and average revenue figures, superhost rates, and unique host counts are calculated.
Trend Analysis: The percentage change in active and inactive listings from month to month that enables the detection of growth and trends in the neighbourhood

![15](https://github.com/user-attachments/assets/1572cce4-8d82-4a38-9de0-159279a1a0b9)
Figure 7.1 dm_listing_neighbourhood table csv

### b. Dm_property_type

The dm_property_type view provides information on listing performance based on property attributes by classifying data by property_type, room_type, accommodates, and month_year.
Key metrics:
Price and Occupancy Trends: Contains the projected income for current listings, the total number of stays, and the minimum, maximum, median, and average prices.
Host and Review Metrics: Provides a quality indicator for every kind of property by displaying the average review ratings, distinct host count, and superhost rate.
Trend Analysis: Shows variations in market demand through monthly percentage changes in active and inactive listings by type of property.

![16](https://github.com/user-attachments/assets/4ce2dad7-fd23-4d43-ad84-89947baa682b)
Figure 7.2 dm_property_type table csv

### c. Dm_host_neighbourhood

The datamart views answer several key business questions:
Revenue and Price Trends: Stakeholders can monitor the most lucrative neighbourhoods or property types by looking at average price and average predicted revenue per active listing.
Host Engagement: Particularly in areas with high visitor demand, the distinct_hosts counts and superhost_rate statistic provide evaluation of host quality and activity.
Occupancy and Demand: Monitoring shifts in active_listings and inactive_listings reveals possible growth regions and offers insights into occupancy trends.
With these perspectives, the Gold layer provides useful information about Airbnb listings, allowing for data-driven choices to increase income and enhance visitor pleasure.

![17](https://github.com/user-attachments/assets/582d95c0-c12d-45d4-aa0f-5a3ab9b8c49d)
Figure 7.3 dm_host_neighbourhood table csv

## 8. End-to-End Orchestration (Airflow DAG)

The automated coordination of data intake, transformation, and loading using Apache Airflow is a crucial component of this project's design. From raw intake to analytics-ready views, data flows effectively thanks to our end-to-end orchestration, which guarantees a dependable and seamless data pipeline.

### a. DAG Design and Sequential Data Processing

The following objectives guide the coordination of data input and transformation via the Airflow DAG, load_to_bronze_schema_sequential:
Chronological Data Loading: To guarantee that each month's data is handled in the correct sequence, the DAG is set up to load Airbnb monthly data files sequentially. Maintaining chronological integrity is crucial, particularly for time-based metrics like number_of_reviews and monthly pricing.
Static Data Handling: First, static datasets are loaded, such as census and LGA mappings. This information serves as the basis for joining and aggregations, which enable listings to be precisely linked to administrative areas and demographic data.

![18](https://github.com/user-attachments/assets/81faa39b-2310-4457-b97b-75bcc58612d1)
Figure 8.1 Airflow DAG execution of all monthly listings data

### b. Triggering dbt Transformations

The DAG initiates a dbt Cloud task to carry out transformations when data loading is finished. By automating the data transfer from the Bronze layer (raw ingestion) to the Silver and Gold levels, this connection with dbt Cloud guarantees fast and reliable updates for all models.
API-Based Task: The DAG pulls the required credentials from Airflow Variables for security and uses a PythonOperator to initiate the dbt Cloud task through an API request. Strong error handling is provided by this method, which logs any failures and raises exceptions to alert the team to problems.
Dependencies and Data Integrity: The DAG guarantees that data is modified only when ingestion is finished, preserving data integrity throughout the pipeline, by imposing dependencies between jobs.

![19](https://github.com/user-attachments/assets/92a08a44-371f-4976-b49d-a39d3d61be7a)
Figure 8.2 Airflow Variables for API trigger (dbtcloud)

![20](https://github.com/user-attachments/assets/371d0eac-98f1-4adc-a97c-a6bc25225f7a)
Figure 8.3 dbtcloud jobs

![21](https://github.com/user-attachments/assets/7ac26f39-3e43-4dc9-92d0-4b9d1307f1f9) 
Figure 8.4 Successful dbtcloud job run

![22](https://github.com/user-attachments/assets/d87134ea-90b5-4172-bd3c-01307276810d)
Figure 8.5 dbtcloud job run results

![23](https://github.com/user-attachments/assets/b2b660e2-5900-4230-a118-f23f4293a7a7)
Figure 8.6 Logs of dbtcloud trigger

![24](https://github.com/user-attachments/assets/82e68eea-b285-4fff-9985-4067c165722b)
Figure 8.7 Airflow DAG status

### c. Monitoring and Error Handling

The way the Airflow DAG is set up to deal with faults is by:
Retries and Alerts: To make sure that team members are informed in the event of problems, the DAG's default_args setting incorporates email notifications upon failure as well as retries for temporary failures.
Logging: Every job keeps track of its progress, giving insight into the transformation and loading procedures. Especially during data import, this recording is useful for pipeline monitoring and problem diagnosis.
By minimising manual involvement and guaranteeing that all transformations and views are promptly updated for precise analysis, this automated orchestration streamlines the data flow.

## 9. Challenges and Solutions

This project encountered several technical challenges, each addressed with strategic solutions to ensure data integrity, automation, and performance.

### a. Sequential Data Loading and Order Integrity
Challenge: Maintaining chronological correctness in the dataset, particularly for time-dependent studies, required that Airbnb data be fed sequentially per month.
Solution: To enforce a rigorous sequence, the Airflow DAG was constructed with dependencies between monthly data loading operations. Processing of the data file for each month didn't start until the preceding file was completely loaded. The integrity of time-based measurements and trends was preserved by this sequential processing.

### b. Data Consistency and Type Validation
Challenge: A number of data types and certain inconsistent or missing values (such as wrong dates or negative prices) were included in the raw Airbnb dataset, which may have affected analysis and transformations.
Solution: For fields like price, review_scores_rating, and availability_30, the Silver layer changes used range checks, null handling, and casting. In order to maintain a clean and consistent dataset for analysis, dbt tests were also included to check data types and make sure needed fields weren't null.

### c. Integration of Airflow and dbt Cloud
Challenge: To integrate Airflow with dbt Cloud in a secure and dependable manner so that dbt transformations would start instantly after data loading.
Solution: via a PythonOperator and dbt credentials safely kept in Airflow Variables, Airflow initiated dbt Cloud tasks via an API-based methodology. This method made sure that the dbt transformations only executed once the data intake process was successfully finished.

### d. Data Integrity in Slowly Changing Dimensions (SCD2)
Challenge: Some tables, such Airbnb listings and census demographics, required historical monitoring in order to be analysed over time.
Solution: SCD Type 2 was set up to record historical changes in particular fields in dbt snapshots. As a result, precise longitudinal analysis was made possible, supporting time-based patterns and demographic shifts in the data with historical context.


## 10. Conclusion
Using Google Cloud Storage, PostgreSQL, Apache Airflow, and dbt Cloud, this project effectively developed a fully coordinated data pipeline that was organised in accordance with the Medallion Architecture (Bronze, Silver, Gold). The pipeline ensures a stable flow of data from raw input to analytical insights by utilising an effective, tiered design that facilitates automated data ingestion, transformation, and aggregation.
Datamart views and the star schema in the Gold layer offer useful information about neighbourhood demographics and Airbnb listings. Targeted measurements and optimised data formats efficiently address business issues about occupancy trends, revenue trends, and property performance.
The project is positioned as a scalable solution for continuous Airbnb and demographic data analysis because of its automation, data validation, and historical recording, which allow for accurate, repeatable analysis. Future improvements, like adding more years to the dataset or including other pertinent data sources, may be built upon the strong foundation this pipeline offers.
