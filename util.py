import polars as pl
from pymongo import MongoClient
from datetime import date
from datetime import datetime

# Expected number of new case IDs to generate per county-year
BATCH_SIZE = {
        "Douglas" : 30,
        "Lancaster": 20,
        "Sarpy": 10
        }

# Mapping from county code to county name
COUNTY_MAP = {
    "01": "Douglas",
    "02": "Lancaster",
    "59": "Sarpy"
}

# Aggregation pipeline to find the highest case number per (year, county)
# Only consider this year 
AGG_PIPELINE = [
    {
        "$group": {
            "_id": {"CaseYear": "$CaseYear", "County": "$County"},
            "MaxCaseNumber": {"$max": {"$toInt": "$CaseNumber"}}
        }
    }
]

# ---------------------------
# Helper Functions
# ---------------------------

def parse_case_info(case_str: str, county_map: dict = COUNTY_MAP) -> dict:
    """
    Parse a case identifier string into its components.

    Example input: "D 01 JV 25 0000123"
    Returns: {
        "CaseYear": 2025,
        "County": "Douglas",
        "CaseNumber": "0000123"
    }
    """
    parts = case_str.split()
    # parts[1] is county code, parts[3] is two-digit year, parts[4] is case number
    county_code = parts[1]
    year_suffix = parts[3]
    case_number = parts[4]

    # Convert '25' -> 2025
    year = 2000 + int(year_suffix)
    county = county_map.get(county_code, "Unknown")

    return {
        "CaseYear": year,
        "County": county,
        "CaseNumber": case_number
    }


def get_next_n_cases(MONGO_URI) -> pl.DataFrame:
    """
    Generate the next batch of case IDs for the current calendar year.
    If a county has no records for this year yet, numbering starts at 0000001.
    """
    client = MongoClient(MONGO_URI)
    db = client["Cluster0"]
    collection = db["Cases"]
    checkpoints = list(collection.aggregate(AGG_PIPELINE))

    target_year = date.today().year
    inv_county_map = {v: k for k, v in COUNTY_MAP.items()}

    max_by_county = {}
    for ckpt in checkpoints:
        if ckpt["_id"]["CaseYear"] != target_year:
            continue
        county = ckpt["_id"]["County"]
        max_by_county[county] = int(ckpt["MaxCaseNumber"])

    raw_ids = []
    year_suffix = str(target_year - 2000)
    for county, batch_size in BATCH_SIZE.items():
        county_code = inv_county_map.get(county, "00")
        start_num = max_by_county.get(county, 0) + 1
        print(
            f"Generating {batch_size} {target_year} {county} cases "
            f"starting at {str(start_num).zfill(7)}"
        )

        for offset in range(batch_size):
            num_str = str(start_num + offset).zfill(7)
            raw_ids.append(f"D {county_code} JV {year_suffix} {num_str}")

    df = pl.DataFrame({"CaseID": raw_ids})
    
    df = df.with_columns(
    pl.col("CaseID").map_elements(
        parse_case_info,
        return_dtype=pl.Struct([
            pl.Field("CaseYear", pl.Int64),
            pl.Field("County", pl.Utf8),
            pl.Field("CaseNumber", pl.Utf8)
        ])
    ).alias("parsed"),
    pl.lit(datetime.utcnow()).alias("TimeScraped"),
    pl.lit(None).alias("Docket"),
    pl.lit(None).alias("DateOfBirth")
).unnest("parsed")

    return df
# util.py
from pymongo import MongoClient
import polars as pl
from datetime import datetime

import polars as pl
from pymongo import MongoClient

def get_bounced_cases(mongo_uri: str) -> pl.DataFrame:
    """
    Fetch bounced cases from MongoDB and return as a Polars DataFrame.
    """
    # Connect to MongoDB
    client = MongoClient(mongo_uri)

    # Use the correct database and collection
    db = client["Cluster0"]  # <-- correct database
    collection = db["Cases"]

    # Fetch all documents
    docs = list(collection.find({}))
    print(f"Total documents fetched from MongoDB: {len(docs)}")

    if not docs:
        print("No documents found in collection.")
        return pl.DataFrame([])  # return empty DataFrame

    # Convert to Polars DataFrame
    df = pl.DataFrame(docs)

    # Optional: filter for "bounced" cases if you have a specific condition
    # For example, if bounced cases are those where 'Docket' is empty or null:
    if "Docket" in df.columns:
        df = df.filter(df["Docket"].is_null() | (df["Docket"] == ""))

    print(f"Shape of DataFrame after filtering bounced cases: {df.shape}")
    print("Columns:", df.columns)
    print("First 10 rows:")
    print(df.head(10))

    return df