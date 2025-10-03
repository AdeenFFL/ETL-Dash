import logging
import time
from datetime import datetime as datetime
import pandas as pd
import numpy as np
from pymongo import MongoClient, UpdateOne
from bson import ObjectId
import importlib
import pricing
importlib.reload(pricing)  # Ensure latest version is used
from pricing import attach_prices
import tqdm

# -------------------------------
# LOGGING SETUP
# -------------------------------
logging.basicConfig(
    filename="etl_run.log",  # log file will be created in same folder
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------------
# DB CONNECTIONS
# -------------------------------

LIVE_CONN_STR = "mongodb+srv://FFL-User:OoesBMAcjYp4pJGf@fflcluster.2mt2rev.mongodb.net/?retryWrites=true&w=majority&appName=FFLCluster&tls=true"
REPORTING_CONN_STR = "mongodb://localhost:27017"

live_client = MongoClient(LIVE_CONN_STR)
reporting_client = MongoClient(REPORTING_CONN_STR)

live_db = live_client["ffl"]
reporting_db = reporting_client["staging_db"]

# -------------------------------
# HELPERS
# -------------------------------
def to_objectid_safe(x):
    if pd.isnull(x):
        return None
    if isinstance(x, ObjectId):
        return x
    if isinstance(x, str):
        try:
            return ObjectId(x)
        except Exception:
            return None
    return None 

def sanitize_datetimes(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: (
                    x.to_pydatetime() if isinstance(x, pd.Timestamp) and pd.notna(x)
                    else (x if isinstance(x, datetime) else None)
                )
            )
    return df

def get_last_run(collection_name):
    meta = reporting_db["etl_metadata"].find_one({"_id": collection_name})
    if meta and "last_run" in meta:
        ts = meta["last_run"]
        if isinstance(ts, pd.Timestamp):
            return ts.to_pydatetime()
        return ts
    return None

def update_last_run(timestamp, collection_name):
    if pd.isna(timestamp):
        safe_ts = None
    elif isinstance(timestamp, pd.Timestamp):
        safe_ts = timestamp.to_pydatetime()
    elif isinstance(timestamp, datetime):
        safe_ts = timestamp
    else:
        safe_ts = None

    reporting_db["etl_metadata"].update_one(
        {"_id": collection_name},
        {"$set": {"last_run": safe_ts}},
        upsert=True
    )

def extract_incremental_purchases(db, collection_name, last_run):
    print("starting extract_incremental_purchases")
    if last_run:
        # Reprocess 8 days back from last_run
        window_start = last_run - pd.Timedelta(days=12)
        query = {"created_at": {"$gte": window_start}, "deleted_at": {"$exists": False}}
    else:
        print("No last_run found, performing full extract")
        # Initial load → extract everything
        query = {"deleted_at": {"$exists": False}}

    fields = {
        "_id": 1,
        "supplier_id": 1,
        "supplier_type_id": 1,
        "mcc_id": 1,
        "area_office_id": 1,
        "gross_volume": 1,
        "ts_volume": 1,
        "opening_balance": 1,
        "type": 1,
        "created_by": 1,
        "updated_at": 1,
        "created_at": 1,
        "booked_at": 1,
        "time": 1,
        "serial_number": 1,
        "is_planned": 1,
        "is_exceptional_release": 1,
        "tests": 1,
        "plant_id": 1,
        "price": 1,
        "cp_id": 1
    }
     # Count total docs first
    count = db[collection_name].count_documents(query)
    print(f"Query matched {count} docs, fetching...")

    # Fetch with progress bar
    cursor = db[collection_name].find(query, fields).batch_size(10000)
    
    batch = []
    for doc in tqdm.tqdm(cursor, total=count, desc="Fetching docs"):
        batch.append(doc)

    df = pd.DataFrame(batch)

    # Ensure expected columns exist
    for col in ["mcc_id", "supplier_id", "supplier_type_id", "area_office_id", "plant_id", "cp_id", "price"]:
        if col not in df.columns:
            df[col] = None

    print(f"✅ Done. Final DataFrame shape: {df.shape}")
    return df


def transform_purchases(purchases_df):
    print("starting transform_purchases")
    if purchases_df.empty:
        return purchases_df
    
    purchases_df["created_at"] = pd.to_datetime(purchases_df["created_at"], errors="coerce")
    purchases_df["updated_at"] = pd.to_datetime(purchases_df["updated_at"], errors="coerce")
    purchases_df["booked_at"] = pd.to_datetime(purchases_df["booked_at"], errors="ignore")
    # purchases_df["deleted_at"] = pd.to_datetime(purchases_df["deleted_at"], errors="coerce")
    # purchases_df["deleted_at"] = purchases_df["deleted_at"].datetime.tz_localize(None)
    purchases_df["time"] = pd.to_datetime(purchases_df["time"], errors="coerce")

    null_mask = purchases_df["booked_at"].isna()
    if null_mask.any():
        logging.info(f"Replacing {int(null_mask.sum())} null booked_at values with time")
        purchases_df.loc[null_mask, "booked_at"] = purchases_df.loc[null_mask, "time"]
    # purchases_df = purchases_df[purchases_df["deleted_at"].isin([None, pd.NaT, np.nan])]
    suppliers_df = pd.DataFrame(list(live_db["suppliers"].find({})))
    suppliers_df=suppliers_df.rename(columns={"area_office":"area_office_id_suppliers"})
    suppliers_df["area_office_id_suppliers"] = suppliers_df["area_office_id_suppliers"].apply(to_objectid_safe)

    collection_points_df = pd.DataFrame(list(live_db["collection_points"].find({}, {
        "_id": 1, "name": 1, "area_office_id": 1, "status": 1,
        "is_mcc": 1, "latitude": 1, "longitude": 1, "address": 1
    })))
    area_offices_df = pd.DataFrame(list(live_db["area_offices"].find({}, {"_id": 1, "name": 1})))
    supplier_types_df = pd.DataFrame(list(live_db["supplier_types"].find({}, {"_id": 1, "name": 1, "description": 1})))

    # Convert IDs
    purchases_df["_id"] = purchases_df["_id"].apply(to_objectid_safe)
    suppliers_df["_id"] = suppliers_df["_id"].apply(to_objectid_safe)
    collection_points_df["_id"] = collection_points_df["_id"].apply(to_objectid_safe)
    collection_points_df["area_office_id"] = collection_points_df["area_office_id"].apply(to_objectid_safe)
    area_offices_df["_id"] = area_offices_df["_id"].apply(to_objectid_safe)
    supplier_types_df["_id"] = supplier_types_df["_id"].apply(to_objectid_safe)

    suppliers_df["supplier_type_id"] = suppliers_df["supplier_type_id"].apply(to_objectid_safe)
    purchases_df["supplier_id"] = purchases_df["supplier_id"].apply(to_objectid_safe)
    purchases_df["mcc_id"]=purchases_df["mcc_id"].fillna(purchases_df["cp_id"])
    purchases_df["mcc_id"] = purchases_df["mcc_id"].apply(to_objectid_safe)
    purchases_df["supplier_type_id"] = purchases_df["supplier_type_id"].apply(to_objectid_safe)
    

    # Select relevant columns
    suppliers_df  = suppliers_df[["_id", "name", "supplier_type_id", "source", "area_office_id_suppliers", "code"]]
    collection_points_df = collection_points_df[["_id", "name", "area_office_id", "is_mcc", "latitude", "longitude"]]
    area_offices_df = area_offices_df[["_id", "name"]]
    supplier_types_df = supplier_types_df[["_id", "name", "description"]]

    # Joins
    purchases_df = purchases_df.merge(
        suppliers_df.rename(columns={"_id": "supplier_id", "name": "supplier_name"}),
        on="supplier_id", how="left", suffixes=("", "_sup")
    )
    purchases_df["area_office_id"]= purchases_df["area_office_id"].fillna(purchases_df["area_office_id_suppliers"])

    purchases_df = purchases_df.merge(
        collection_points_df.rename(columns={"_id": "mcc_id", "name": "collection_point_name"}),
        on="mcc_id", how="left", suffixes=("", "_mcc")
    )
    purchases_df["area_office_id"]= purchases_df["area_office_id"].fillna(purchases_df["area_office_id_mcc"])

    purchases_df = purchases_df.merge(
        area_offices_df.rename(columns={"_id": "area_office_id", "name": "area_office_name"}),
        left_on="area_office_id",right_on="area_office_id", how="left", suffixes=("", "_ao")
    )

    purchases_df = purchases_df.merge(
        supplier_types_df.rename(columns={"_id": "supplier_type_id", "name": "supplier_type_name"}),
        on="supplier_type_id", how="left"
    )

    purchases_df = purchases_df.rename(columns={"area_office_id": "area_office_id_ao"})
    purchases_df["price"] = purchases_df["price"].replace({np.nan: None})  

    # purchases_df["price"] = purchases_df["price"].astype(float)
    return purchases_df

def load_to_reporting(df, collection_name):
    print("starting load_to_reporting")
    if df.empty:
        logging.info("⚠️ Nothing to load")
        return
    print("here 1")
    # Convert timestamps and replace NaNs with None
    df = sanitize_datetimes(df)
    df = df.where(pd.notnull(df), None)
    print("here 2")
    # Drop rows with null _id (safety)
    df = df[df["_id"].notnull()]
    # df = df[df["deleted_at"].isin([None, pd.NaT, np.nan])]
    print("here 3")
    ops = []
    for _, row in df.iterrows():
        doc = row.to_dict()
        _id = doc.pop("_id")
        ops.append(UpdateOne({"_id": _id}, {"$set": doc}, upsert=True))
    print("here 4")
    # Run bulk write
    result = reporting_db[collection_name].bulk_write(ops, ordered=False)
    print("here 5")
    # More detailed logging
    inserted_count = len(result.upserted_ids)
    updated_count = result.modified_count
    matched_but_unchanged = result.matched_count - updated_count
    
    logging.info(
        f"✅ Load completed for {collection_name}: "
        f"inserts={inserted_count}, updates={updated_count}, unchanged={matched_but_unchanged}"
    )
# def load_to_reporting(df, collection_name):
#     if df.empty:
#         logging.info("⚠️ Nothing to load")
#         return

#     df = sanitize_datetimes(df)
#     df = df.where(pd.notnull(df), None)

#     df = df[df["_id"].notnull()]
#     ops = []
#     for _, row in df.iterrows():
#         doc = row.to_dict()
#         _id = doc.pop("_id")
#         ops.append(UpdateOne({"_id": _id}, {"$set": doc}, upsert=True))

#     result = reporting_db[collection_name].bulk_write(ops, ordered=False)
#     logging.info(f"✅ Load completed: matched={result.matched_count}, modified={result.modified_count}, upserted={len(result.upserted_ids)}")

# -------------------------------
# MAIN ETL RUN
# -------------------------------
def run_etl(collection_name):
    start_time = time.time()
    logging.info(f"ETL started for {collection_name}")

    last_run = get_last_run(collection_name)
    logging.info(f"Last run checkpoint: {last_run}")

    try:
        # 1. Extract
        if collection_name == "milk_purchases":
            relevant_df = extract_incremental_purchases(live_db, collection_name, last_run)
            logging.info(f"Extracted {len(relevant_df)} new/updated purchases")
        else:
            logging.warning(f"No extraction defined for collection: {collection_name}")
            relevant_df = pd.DataFrame()

        # 2. Transform
        if collection_name == "milk_purchases":
            transformed_df = transform_purchases(relevant_df)
            logging.info(f"Transformed {len(transformed_df)} rows")

            prices_df = pd.DataFrame(list(live_db["prices"].find({"status": 1})))
            archived_prices_df = pd.DataFrame(list(live_db["archieved_prices"].find({"status": 1})))

            for df in [prices_df, archived_prices_df]:
                for col in ["plant", "source_type", "area_office", "supplier", "collection_point"]:
                    if col in df.columns:
                        df[col] = df[col].apply(to_objectid_safe)

            transformed_df["price_before_attach"] = transformed_df["price"]

            transformed_df = attach_prices(transformed_df, prices_df, archived_prices_df)
            logging.info("💰 Prices attached")

        # 3. Load
        load_to_reporting(transformed_df, "fact_"+collection_name)
        print("here 6")
        # 4. Update checkpoint
        if not relevant_df.empty:
            new_last_run = relevant_df["created_at"].max()
            update_last_run(new_last_run, collection_name)
            logging.info(f"Updated last_run checkpoint to {new_last_run}")
        else:
            logging.info("No new data to process")
        print("here 7")
        elapsed = time.time() - start_time
        logging.info(f"ETL finished for {collection_name} in {elapsed:.2f} seconds")

    except Exception as e:
        elapsed = time.time() - start_time
        logging.error(f"ETL failed for {collection_name} after {elapsed:.2f} seconds. Error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_etl("milk_purchases")
