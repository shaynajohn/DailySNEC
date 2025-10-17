from pymongo import MongoClient
from pprint import pprint

# Connect to your cluster
client = MongoClient("mongodb+srv://shaynacjohn:test13@cluster0.hiq0tdr.mongodb.net/")

# Use the correct database
db_name = "Cluster0"
db = client[db_name]

# List collections in this database
print(f"Collections in {db_name}:")
print(db.list_collection_names())

# Choose the collection
collection_name = "Cases"
collection = db[collection_name]

# Count documents in the collection
print(f"Number of documents in {collection_name}: {collection.count_documents({})}")

# Fetch 10 most recent documents by TimeScraped
recent_cases = collection.find({}, {"CaseID": 1, "TimeScraped": 1}).sort("TimeScraped", -1).limit(10)
print("Most recent 10 TimeScraped values:")
for case in recent_cases:
    pprint(case)
