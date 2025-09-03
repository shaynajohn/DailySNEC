import polars as pl
from pymongo import MongoClient
from datetime import date

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
    Generate the next batch of case IDs to scrape based on stored data.

    1. Query MongoDB for the current max case number per (year, county).
    2. Build the next BATCH_SIZE IDs for each group.
    3. Parse each into structured columns using Polars.
    4. Add metadata columns (TimeScraped, Docket, DateOfBirth).
    """
    # Connect to MongoDB and aggregate
    client = MongoClient(MONGO_URI)
    db = client["JVCases"]
    collection = db["Cases"]
    checkpoints = list(collection.aggregate(AGG_PIPELINE))

    # Build reverse map from county name back to code
    inv_county_map = {v: k for k, v in COUNTY_MAP.items()}

    # Generate next IDs
    raw_ids = []
    for ckpt in checkpoints:

        if ckpt["_id"]["CaseYear"] != date.today().year:
            continue

        year_suffix = str(ckpt["_id"]["CaseYear"] - 2000)
        county_code = inv_county_map.get(ckpt["_id"]["County"], "00")
        # next sequential case numbers, zero-padded to 7 digits
        start_num = int(ckpt["MaxCaseNumber"]) + 1

        batch_size = BATCH_SIZE[ckpt["_id"]["County"]]

        for offset in range(batch_size):
            num_str = str(start_num + offset).zfill(7)
            raw_ids.append(f"D {county_code} JV {year_suffix} {num_str}")

    # Create DataFrame and parse into columns
    df = pl.DataFrame({"CaseID": raw_ids})
    df = df.with_columns(
    pl.col("CaseID").map_elements(
        parse_case_info, 
        return_dtype=pl.Struct([
            pl.Field("CaseNumber", pl.String),
            pl.Field("CaseYear", pl.Int64),
            pl.Field("CaseType", pl.String)
        ])
    ).alias("parsed"),
        pl.lit(date.today()).cast(pl.Datetime).alias("TimeScraped"),
        pl.lit(None).alias("Docket"),
        pl.lit(None).alias("DateOfBirth")
).unnest("parsed")
    

    return df

def get_bounced_cases(MONGO_URI) -> pl.DataFrame:
    client = MongoClient(MONGO_URI)
    db = client["JVCases"]
    collection = db["Cases"]

    cursor = collection.find()  
    data   = list(cursor) 

    not_scraped = [parse_case_info(d["CaseID"]) for d in data if "Case Summary" not in d["Docket"]]

    df = pl.DataFrame({"parsed": not_scraped})
    df = df.with_columns(
            pl.lit(date.today()).cast(pl.Datetime).alias("TimeScraped"),
            pl.lit(None).alias("Docket"),
            pl.lit(None).alias("DateOfBirth")
        ).unnest("parsed")
    
    print(df)

    return df