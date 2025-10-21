Python-ETL-Pipeline-Project
Project Overview

This project is part of a group assignment focused on building a complete Data Pipeline and ETL (Extract, Transform, Load) process using Python and PostgreSQL. The aim is to gain hands-on experience in data engineering by processing a large dataset (minimum 2 million records) relevant to a real-world business scenario.

Objectives

Select a large dataset aligned with a business problem or domain.

Design and implement an ETL process using Python (Pandas).

Store the transformed data in a PostgreSQL database with a well-designed schema.

Ensure the pipeline is modular, efficient, and reproducible.

Project Components
1. Data Source

A dataset containing over 2 million records.

Chosen based on relevance to a real-world business use case (e.g., customer analytics, sales data, transaction logs, etc.).

2. ETL Process

Extract: Load raw data using Pandas from CSV, JSON, or an API.

Transform: Clean, normalize, and format the data to ensure quality and consistency.

Load: Insert the transformed data into PostgreSQL using SQLAlchemy or psycopg2.

3. Database Management

PostgreSQL used for persistent data storage.

Database schema designed based on dataset structure and business requirements.

Indexed and normalized to support efficient querying and analysis.

Technologies Used

Python (Pandas, SQLAlchemy, psycopg2)

PostgreSQL

Jupyter Notebook / Python Scripts

Git for version control

Setup Instructions

Clone the Repository

https://github.com/Gattuoch/Python-ETL-Pipeline-Project.git
cd Python-ETL-Pipeline-Project


Create a Virtual Environment

python -m venv venv
source venv/bin/activate  # For Unix/macOS
venv\Scripts\activate     # For Windows


Install Dependencies

pip install -r requirements.txt


Configure Database

Set up a PostgreSQL database.

Update connection settings in config.py or .env file.

Run the ETL Pipeline

python etl_pipeline.py

Group Members
Name	            ID
Gattuoch Chambang	1401298
Danial Baye	        1401105
Abdihakiim Mohamed	368713