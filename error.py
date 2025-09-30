from util import get_bounced_cases
from pymongo import MongoClient
import polars as pl

MONGO_URI = "mongodb+srv://shaynacjohn:test13@cluster0.hiq0tdr.mongodb.net"

# Fetch bounced cases
df_new = get_bounced_cases(MONGO_URI)

print("===================================================")
print("Next cases returned by get_bounced_cases():")
if df_new.is_empty():
    print("⚠️ No bounced cases returned! This means scraper has nothing to process.")
else:
    # Show first 10 rows
    print(df_new.head(10))
print("Total cases returned:", df_new.height)
print("===================================================")
