import pandas as pd
from pymongo import MongoClient
from bson import ObjectId

def safe_to_objectid(val):
    """Convert 24-char hex string to ObjectId, otherwise return as-is."""
    if isinstance(val, str) and len(val) == 24:
        try:
            return ObjectId(val)
        except Exception:
            return val
    return val

def main():
    # 1. Connect to MongoDB
    LIVE_CONN_STR = "mongodb+srv://FFL-User:OoesBMAcjYp4pJGf@fflcluster.2mt2rev.mongodb.net/?retryWrites=true&w=majority&appName=FFLCluster&tls=true"
    REPORTING_CONN_STR = "mongodb://localhost:27017"

    live_client = MongoClient(LIVE_CONN_STR)
    reporting_client = MongoClient(REPORTING_CONN_STR)

    live_db = live_client["ffl"]
    reporting_db = reporting_client["staging_db"]

    # DB + collection handles
    reporting_col = live_client["ffl"]["milk_purchase_reporting_facts"]
    staging_col = reporting_client["staging_db"]["fact_milk_purchases"]

    # 2. Load into DataFrames
    reporting_df = pd.DataFrame(
        list(reporting_col.find({}, {"purchase_id": 1, "base_price": 1, "_id": 0}))
    )
    staging_df = pd.DataFrame(
        list(staging_col.find({}))  # fetch EVERYTHING
    )

    print(f"Reporting facts: {len(reporting_df)} rows")
    print(f"Staging facts: {len(staging_df)} rows")

    # 3. Convert purchase_id into ObjectId (if string hex)
    reporting_df["purchase_id"] = reporting_df["purchase_id"].apply(safe_to_objectid)

    # 4. Merge on purchase_id vs _id
    merged = staging_df.merge(
        reporting_df,
        left_on="_id",
        right_on="purchase_id",
        how="inner"
    )
    print(f"Merged: {len(merged)} rows")

    if merged.empty:
        print("⚠️ No matches found! Check if IDs are in different formats.")
        return

    # 5. Ensure numeric types for price comparison
    merged["price"] = pd.to_numeric(merged["price"], errors="coerce")
    merged["base_price"] = pd.to_numeric(merged["base_price"], errors="coerce")

    # 6. Compute difference
    merged["price_diff"] = merged["base_price"] - merged["price"]

    # 7. Find mismatches
    mismatches = merged[merged["price_diff"].abs() > 0.0001]  # tolerance

    print(f"Mismatches: {len(mismatches)}")

    print("\nSample mismatches:")
    mismatch_cols = ["_id", "purchase_id", "type", "booked_date", "price", "base_price", "price_diff"]
    available_cols = [col for col in mismatch_cols if col in mismatches.columns]
    print(mismatches[available_cols].head(10))

    # 8. Save results — now includes *all staging columns*
    # merged.to_csv("price_comparison.csv", index=False)
    mismatches.to_csv("price_mismatches.csv", index=False)

    print("\nResults saved to price_comparison.csv and price_mismatches.csv")

if __name__ == "__main__":
    main()
