# IPL Data Analysis & ETL Pipeline Project

<img width="1009" alt="Image" src="https://github.com/user-attachments/assets/1fadc89a-03a3-40f5-8201-cc9bb0879516" />

## Project Overview
This project demonstrates a complete ETL (Extract, Transform, Load) pipeline implementation for analyzing Indian Premier League (IPL) cricket data from 2008 to 2017. The pipeline leverages AWS S3 for data storage and Databricks (Community Edition) for data processing and analysis using PySpark, SQL, and Python.

## Data Source
The raw data was sourced from Cricsheet.org in YAML format and converted to CSV format using R Script, SQL, and SSIS by the original data provider. The dataset contains detailed ball-by-ball information for all 637 IPL matches through the 2017 season.

# Technical Implementation

## Data Pipeline Architecture

### 1.Data Ingestion:
Uploaded CSV files to AWS S3 using AWS CLI
Files include:
Ball_By_Ball.csv (detailed ball-by-ball records)
Match.csv (match metadata and outcomes)
Player.csv (player information)
Player_match.csv (player performance by match)
Team.csv (team information)

### 2.Data Processing:
Connected AWS S3 to Databricks workspace
Created PySpark notebooks in Databricks Community Edition
Implemented schema design and data validation
Performed data cleaning and transformation

### 3.Analysis:
Conducted exploratory data analysis using PySpark DataFrames
Executed SQL queries for specific insights
Created visualizations to uncover patterns and trends


# Key Skills Demonstrated

AWS S3: Cloud storage management via CLI\
Databricks: Notebook creation and execution\
PySpark:\
  Schema design and enforcement\
  Data manipulation and transformation\
  Aggregation operations\
SQL: Complex queries for cricket analytics\
Python: Data analysis and visualization\
ETL Pipeline Development: End-to-end data workflow implementation








