import pandas as pd
from sqlalchemy import create_engine


class DataExtractor:
    """
    Extracts data from CSV files or a database.
    Can yield chunks or return full DataFrames.
    """

    def __init__(self, csv_path=None, db_url=None):
        self.csv_path = csv_path
        self.db_url = db_url

    def extract_from_csv(self, chunksize=100000):
        """Yield chunks of data from a CSV file."""
        if not self.csv_path:
            print("⚠️ CSV path not provided.")
            return

        try:
            for i, chunk in enumerate(pd.read_csv(
                self.csv_path,
                chunksize=chunksize,
                encoding="latin1",
                on_bad_lines="skip",
                low_memory=False
            )):
                print(f"✅ Extracted chunk {i+1} with {len(chunk)} rows from {self.csv_path}")
                yield chunk
        except Exception as e:
            print(f"❌ Failed to extract CSV {self.csv_path}: {e}")

    def extract_all_csv(self):
        """Load the full CSV file at once (no chunks)."""
        if not self.csv_path:
            print("⚠️ CSV path not provided.")
            return pd.DataFrame()

        try:
            df = pd.read_csv(self.csv_path, encoding="latin1", on_bad_lines="skip", low_memory=False)
            print(f"✅ Extracted full CSV from {self.csv_path} with {len(df)} rows")
            return df
        except Exception as e:
            print(f"❌ Failed to load full CSV {self.csv_path}: {e}")
            return pd.DataFrame()

    def extract_from_database(self, query, chunksize=100000):
        """Yield chunks of data from a database query."""
        if not self.db_url:
            print("⚠️ Database URL not provided.")
            return

        try:
            engine = create_engine(self.db_url)
            for i, chunk in enumerate(pd.read_sql(query, con=engine, chunksize=chunksize)):
                print(f"✅ Extracted DB chunk {i+1} with {len(chunk)} rows")
                yield chunk
        except Exception as e:
            print(f"❌ Failed to extract from database: {e}")
