import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get database URL
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL not found in .env file.")

# Ensure Neon connection URL format
if DATABASE_URL.startswith("postgresql//"):
    DATABASE_URL = DATABASE_URL.replace("postgresql//", "postgresql+psycopg2://")

# Create SQLAlchemy engine
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("✅ Connected to PostgreSQL database successfully!")
except Exception as e:
    print("❌ Failed to connect to database:", e)
    exit()


def load_to_postgres(df: pd.DataFrame, table_name: str, chunk_size: int = 100000):
    """Loads data to PostgreSQL in chunks."""
    if df.empty:
        print(f"⚠️ Skipping {table_name}: DataFrame is empty.")
        return

    print(f"🚀 Loading {len(df)} rows into '{table_name}' table...")

    try:
        # Load in chunks for efficiency
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            chunk.to_sql(table_name, engine, if_exists="replace" if i == 0 else "append", index=False)
            print(f"✅ Inserted rows {i}–{i+len(chunk)} into {table_name}")
        print(f"🎯 Successfully loaded '{table_name}' table.")
    except Exception as e:
        print(f"❌ Failed to load {table_name}: {e}")


def test_table_count(table_name):
    """Check number of records in a table."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
            count = result.scalar()
            print(f"📊 {table_name}: {count} rows in database.")
    except Exception as e:
        print(f"⚠️ Could not count rows for {table_name}: {e}")


if __name__ == "__main__":
    print("🚀 Starting Data Load Process...")

    try:
        flights = pd.read_csv("../data/flights.csv", low_memory=False)
        airlines = pd.read_csv("../data/airlines.csv")
        airports = pd.read_csv("../data/airports.csv")

        load_to_postgres(airlines, "airlines")
        load_to_postgres(airports, "airports")
        load_to_postgres(flights.head(2000000), "flights")

        print("\n✅ Data successfully loaded into PostgreSQL!")

        # Optional: Verify counts
        test_table_count("airlines")
        test_table_count("airports")
        test_table_count("flights")

    except Exception as e:
        print("❌ Load process failed:", e)
