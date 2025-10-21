"""
main.py — ETL Pipeline Entry Point
Author: Your Name
Description: Runs Extract → Transform → Load using Pandas and PostgreSQL.
"""

from extract import DataExtractor
from transform import DataTransformer
from load import load_to_postgres
import pandas as pd

print("🚀 Starting Big Data ETL Pipeline...")

try:
    # --------------------- EXTRACT ---------------------
    print("\n📥 Extracting data...")
    extractor_flights = DataExtractor(csv_path="../data/flights.csv")
    extractor_airlines = DataExtractor(csv_path="../data/airlines.csv")
    extractor_airports = DataExtractor(csv_path="../data/airports.csv")

    # Read full CSVs (or use chunks for big ones)
    flights = extractor_flights.extract_all_csv()
    airlines = extractor_airlines.extract_all_csv()
    airports = extractor_airports.extract_all_csv()

    print("✅ All datasets extracted successfully.")

    # --------------------- TRANSFORM ---------------------
    print("\n🔧 Transforming data...")

    transformer = DataTransformer()
    flights_clean = transformer.clean_flights(flights)
    airlines_clean = transformer.clean_airlines(airlines)
    airports_clean = transformer.clean_airports(airports)

    print("✅ Transformation completed successfully.")

    # --------------------- LOAD ---------------------
    print("\n🚀 Loading data to PostgreSQL...")
    load_to_postgres(airlines_clean, "airlines")
    load_to_postgres(airports_clean, "airports")
    load_to_postgres(flights_clean.head(2000000), "flights")  # limit to 2M

    print("\n🎯 ETL Pipeline executed successfully!")

except Exception as e:
    print(f"❌ ETL Pipeline failed: {e}")
