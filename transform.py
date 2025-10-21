import pandas as pd
import numpy as np
from typing import Union, Generator


class DataTransformer:
    """
    Cleans and merges flight, airline, and airport datasets.
    Automatically handles both DataFrames and generators (from chunked CSV extraction).
    """

    def __init__(self):
        print("⚙️ DataTransformer initialized...")

    # -----------------------------
    # Utility: Handle Generators
    # -----------------------------
    def _ensure_dataframe(self, data: Union[pd.DataFrame, Generator]) -> pd.DataFrame:
        """Convert generator to DataFrame if needed."""
        if isinstance(data, Generator):
            chunks = list(data)
            if not chunks:
                print("⚠️ No data found in generator.")
                return pd.DataFrame()
            df = pd.concat(chunks, ignore_index=True)
            print(f"✅ Combined {len(chunks)} chunks into one DataFrame with {len(df)} rows.")
            return df
        elif isinstance(data, pd.DataFrame):
            return data
        else:
            raise TypeError("❌ Input must be a pandas DataFrame or a generator yielding DataFrames.")

    # -----------------------------
    # Step 1: Clean Flights
    # -----------------------------
    def clean_flights(self, flights_df: Union[pd.DataFrame, Generator]) -> pd.DataFrame:
        """Clean flight dataset: handle missing values, duplicates, invalid data."""
        print("🔧 Cleaning flights data...")

        flights_df = self._ensure_dataframe(flights_df)
        if flights_df.empty:
            print("⚠️ Empty flights dataset.")
            return flights_df

        # Drop missing values in critical columns
        flights_df = flights_df.dropna(subset=["AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"])

        # Remove duplicates
        before = len(flights_df)
        flights_df = flights_df.drop_duplicates()
        print(f"🧹 Removed {before - len(flights_df)} duplicate rows from flights.")

        # Replace missing numeric delays/cancellations with 0
        numeric_cols = ["DEPARTURE_DELAY", "ARRIVAL_DELAY", "CANCELLED"]
        for col in numeric_cols:
            if col in flights_df.columns:
                flights_df[col] = pd.to_numeric(flights_df[col], errors="coerce").fillna(0)

        # Remove unrealistic delay values
        if "DEPARTURE_DELAY" in flights_df.columns:
            flights_df = flights_df[flights_df["DEPARTURE_DELAY"].between(-60, 1500)]

        if "ARRIVAL_DELAY" in flights_df.columns:
            flights_df = flights_df[flights_df["ARRIVAL_DELAY"].between(-60, 1500)]

        print(f"✅ Flights cleaned successfully. {len(flights_df)} rows remaining.")
        return flights_df

    # -----------------------------
    # Step 2: Clean Airlines
    # -----------------------------
    def clean_airlines(self, airlines_df: Union[pd.DataFrame, Generator]) -> pd.DataFrame:
        """Clean airlines dataset."""
        print("🔧 Cleaning airlines data...")

        airlines_df = self._ensure_dataframe(airlines_df)
        if airlines_df.empty:
            print("⚠️ Empty airlines dataset.")
            return airlines_df

        airlines_df = airlines_df.dropna(subset=["IATA_CODE", "AIRLINE"])
        airlines_df = airlines_df.drop_duplicates()

        print(f"✅ Airlines cleaned successfully. {len(airlines_df)} rows remaining.")
        return airlines_df

    # -----------------------------
    # Step 3: Clean Airports
    # -----------------------------
    def clean_airports(self, airports_df: Union[pd.DataFrame, Generator]) -> pd.DataFrame:
        """Clean airports dataset."""
        print("🔧 Cleaning airports data...")

        airports_df = self._ensure_dataframe(airports_df)
        if airports_df.empty:
            print("⚠️ Empty airports dataset.")
            return airports_df

        airports_df = airports_df.dropna(subset=["IATA_CODE", "AIRPORT", "CITY"])
        airports_df = airports_df.drop_duplicates()

        print(f"✅ Airports cleaned successfully. {len(airports_df)} rows remaining.")
        return airports_df

    # -----------------------------
    # Step 4: Merge All Data
    # -----------------------------
    def merge_datasets(self, flights: pd.DataFrame, airlines: pd.DataFrame, airports: pd.DataFrame) -> pd.DataFrame:
        """Merge the cleaned datasets into one enriched dataset."""
        print("🔗 Merging datasets...")

        if flights.empty or airlines.empty or airports.empty:
            print("⚠️ One or more datasets are empty. Merge may be incomplete.")

        merged = flights.merge(airlines, how="left", left_on="AIRLINE", right_on="IATA_CODE", suffixes=("", "_AIRLINE"))
        merged = merged.merge(airports, how="left", left_on="ORIGIN_AIRPORT", right_on="IATA_CODE", suffixes=("", "_ORIGIN"))
        merged = merged.merge(airports, how="left", left_on="DESTINATION_AIRPORT", right_on="IATA_CODE", suffixes=("", "_DEST"))

        print(f"✅ Merge completed. Final dataset has {len(merged)} rows and {len(merged.columns)} columns.")
        return merged

    # -----------------------------
    # Step 5: Run All Transformations
    # -----------------------------
    def transform_all(self, flights, airlines, airports):
        """Run all cleaning and merging steps."""
        cleaned_flights = self.clean_flights(flights)
        cleaned_airlines = self.clean_airlines(airlines)
        cleaned_airports = self.clean_airports(airports)

        final_df = self.merge_datasets(cleaned_flights, cleaned_airlines, cleaned_airports)
        print("🚀 Transformation pipeline completed successfully.")
        return final_df


# -----------------------------
# Run directly (for testing)
# -----------------------------
if __name__ == "__main__":
    from extract import DataExtractor

    print("🚀 Starting ETL transformation...")

    flights_extractor = DataExtractor(csv_path="../data/flights.csv")
    airlines_extractor = DataExtractor(csv_path="../data/airlines.csv")
    airports_extractor = DataExtractor(csv_path="../data/airports.csv")

    flights = flights_extractor.extract_from_csv()
    airlines = airlines_extractor.extract_from_csv()
    airports = airports_extractor.extract_from_csv()

    transformer = DataTransformer()
    transformed_data = transformer.transform_all(flights, airlines, airports)

    print("\n✅ All datasets transformed successfully.")
    print(transformed_data.head())
