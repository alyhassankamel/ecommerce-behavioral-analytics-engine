import os
import json
from pymongo import MongoClient
import pandas as pd

# Ensure output directory exists
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.makedirs(os.path.join(base_path, 'phase2'), exist_ok=True)

# MongoDB connection (local Docker container)
client = MongoClient('mongodb://localhost:27017/')
db = client['ecommerce']
profiles_coll = db['user_profiles']
cooccurrence_coll = db['item_cooccurrence']

# Load Phase 1 outputs (CSV/JSON)
# Market basket co-occurrence counts
cooccurrence_file = os.path.join(base_path, 'phase1_output', 'market_basket_counts.csv')
if os.path.exists(cooccurrence_file):
    co_df = pd.read_csv(cooccurrence_file)
    # Insert into MongoDB
    records = co_df.to_dict('records')
    cooccurrence_coll.delete_many({})
    cooccurrence_coll.insert_many(records)

# User affinity JSON
affinity_file = os.path.join(base_path, 'phase1_output', 'user_affinity', 'affinity_results.json')
if os.path.exists(affinity_file):
    affinity_records = []
    with open(affinity_file, 'r') as fp:
        for line in fp:
            if line.strip():
                doc = json.loads(line)
                affinity_records.append(doc)
    # Build profile per user: embed top categories as a list of {category, score}
    profiles = {}
    for rec in affinity_records:
        uid = rec['user_id']
        cat = rec['category']
        score = rec['affinity_score']
        profiles.setdefault(uid, []).append({'category': cat, 'score': score})
    # Upsert into MongoDB
    profiles_coll.delete_many({})
    for uid, cats in profiles.items():
        profiles_coll.insert_one({'user_id': uid, 'top_categories': cats})

print('Phase 2 ingestion completed: MongoDB collections populated.')
