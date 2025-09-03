from pymongo import MongoClient
from pprint import pprint
import os

# For local testing with .env (optional)
from dotenv import load_dotenv
load_dotenv()

# Get the connection string from environment variables
mongo_uri = os.getenv("MONGODB_CONNECTION_STRING")

# Connect to your MongoDB cluster
client = MongoClient(mongo_uri)
db = client["JVCases"]
collection = db["Cases"]  # Adjust if needed

# Find 10 most recent documents by TimeScraped
recent_cases = collection.find({}, {"CaseID": 1, "TimeScraped": 1}).sort("TimeScraped", -1).limit(10)

print("Most recent 10 TimeScraped values:")
for case in recent_cases:
    pprint(case)


