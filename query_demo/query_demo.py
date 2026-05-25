import sys
import json
from pymongo import MongoClient

def run_demo(user_id, item_id):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ecommerce"]
    
    # 1. Fetch User Profile
    profile = db["user_profiles"].find_one({"user_id": user_id}, {"_id": 0})
    
    # 2. Fetch Item Co-occurrence (Recommendations)
    # Search for item_id in either item_a or item_b
    recommendations = list(db["item_cooccurrence"].find({
        "$or": [{"item_a": item_id}, {"item_b": item_id}]
    }, {"_id": 0}).sort("cooccurrence_count", -1).limit(5))
    
    # Format recommendations for better viewing
    rec_list = []
    for r in recommendations:
        other_item = r["item_b"] if r["item_a"] == item_id else r["item_a"]
        rec_list.append({
            "recommended_item": other_item,
            "strength": r["cooccurrence_count"]
        })

    result = {
        "query": {
            "user_id": user_id,
            "seed_item": item_id
        },
        "user_profile": profile,
        "recommendations": rec_list
    }
    
    print("\n--- Recommendation Engine Query Demo ---")
    print(json.dumps(result, indent=4))
    print("----------------------------------------\n")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python query_demo.py <user_id> <item_id>")
        print("Example: python query_demo.py User_2686 ITEM_3306")
    else:
        try:
            run_demo(sys.argv[1], sys.argv[2])
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            print("Make sure your MongoDB container is running (docker compose up -d)")
