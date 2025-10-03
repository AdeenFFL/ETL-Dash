from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import bson
from bson import ObjectId
from pymongo import UpdateOne
import importlib
import pricing
importlib.reload(pricing)  # Ensure latest version is used
from pricing import attach_prices

# -------------------------------
# DB CONNECTIONS
# -------------------------------
LIVE_CONN_STR = "mongodb://localhost:27017"
REPORTING_CONN_STR = "mongodb://localhost:27017"

live_client = MongoClient(LIVE_CONN_STR)
reporting_client = MongoClient(REPORTING_CONN_STR)

live_db = live_client["initial_dump"]
reporting_db = reporting_client["staging_db"]


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
    
from datetime import datetime
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
        # Ensure it is a datetime, not pandas Timestamp
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

    
def extract_test_values(tests, target_name):
    """Helper to pull test value for a specific qa_test_name if status==1."""
    if not tests:
        return None
    for t in tests:
        if t.get("qa_test_name") == target_name and t.get("status") == 1:
            return t.get("value")
    return None

def find_base_price(purchase, prices_df, arch_prices_df):
    return None

def find_plant_base_price(purchase, prices_df, arch_prices_df):
    return 

# -------------------------------
# EXTRACT: Incremental from live DB
# -------------------------------
def extract_incremental_purchases(db, collection_name, last_run):
    if last_run:
        query = {"created_at": {"$gte": last_run}}
    else:
        query = {}  # First full load

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
        "created_at": 1,
        "booked_at": 1, 
        "time": 1,
        "serial_number" : 1, 
        "is_planned": 1, 
        "is_exceptional_release": 1,
        "tests": 1,
        "plant_id": 1,
        "price": 1,
        "cp_id": 1
    }
    cursor = db[collection_name].find(query, fields)
    cursor = db[collection_name].find(query, fields)
    df = pd.DataFrame(list(cursor))

    # Ensure expected columns always exist
    for col in ["mcc_id", "supplier_id", "supplier_type_id", "area_office_id", "plant_id", "cp_id"]:
        if col not in df.columns:
            df[col] = None

    
    return df

# -------------------------------
# TRANSFORM: Merge with lookups
# -------------------------------
def transform_purchases(purchases_df):
    if purchases_df.empty:
        return purchases_df
    
    purchases_df["created_at"] = pd.to_datetime(purchases_df["created_at"], errors="coerce")
    purchases_df["booked_at"] = pd.to_datetime(purchases_df["booked_at"], errors="ignore")
    purchases_df["time"] = pd.to_datetime(purchases_df["time"], errors="coerce")

     # Replace null/NaT booked_at with values from 'time' column (if present)
    if "booked_at" in purchases_df.columns and "time" in purchases_df.columns:
        null_mask = purchases_df["booked_at"].isna()
        if null_mask.any():
            print(f"ℹ️ Replacing {int(null_mask.sum())} null booked_at values with time")
            purchases_df.loc[null_mask, "booked_at"] = purchases_df.loc[null_mask, "time"]


    suppliers_df = pd.DataFrame(list(live_db["suppliers"].find({})))
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
    suppliers_df  = suppliers_df[["_id", "name", "supplier_type_id", "source", "area_office", "code"]]
    collection_points_df = collection_points_df[["_id", "name", "area_office_id", "is_mcc", "latitude", "longitude"]]
    area_offices_df = area_offices_df[["_id", "name"]]
    supplier_types_df = supplier_types_df[["_id", "name", "description"]]

    # Joins
    purchases_df = purchases_df.merge(
        suppliers_df.rename(columns={"_id": "supplier_id", "name": "supplier_name"}),
        on="supplier_id", how="left", suffixes=("", "_sup")
    )
    # print("After Suppliers Join:")
    # display(purchases_df.dtypes)
    
    purchases_df = purchases_df.merge(
        collection_points_df.rename(columns={"_id": "mcc_id", "name": "collection_point_name"}),
        on="mcc_id", how="left", suffixes=("", "_mcc")
    )
    # print("After MCC Join:")
    # display(purchases_df.dtypes)

    purchases_df = purchases_df.merge(
        area_offices_df.rename(columns={"_id": "area_office_id", "name": "area_office_name"}),
        left_on="area_office_id_mcc",right_on="area_office_id", how="left", suffixes=("", "_ao")
    )
    purchases_df = purchases_df.merge(
        supplier_types_df.rename(columns={"_id": "supplier_type_id", "name": "supplier_type_name"}),
        on="supplier_type_id", how="left"
    )
    purchases_df["area_office_id_ao"]=purchases_df["area_office_id_ao"].fillna(purchases_df["area_office_id"])
    # Ensure datetime safe
    # purchases_df["booked_date"] = pd.to_datetime(purchases_df["booked_date"], errors="coerce")

    # Drop duplicates of source columns only if they exist
    for col in ["area_office", "source"]:
        if col in purchases_df.columns:
            purchases_df = purchases_df.drop(columns=[col])

  #  print(f"transform_purchases → rows={len(purchases_df)}, with _id={purchases_df['_id'].notna().sum()}")
    # display(purchases_df.head(2))
    # display(purchases_df.dtypes)
    return purchases_df


def load_to_reporting(df, collection_name):
    if df.empty:
      #  print("⚠️ Nothing to load")
        return

    df = sanitize_datetimes(df)   # <- NEW LINE
    df = df.where(pd.notnull(df), None)
    ## print(df["booked_date"].unique()[:10])
    ## print(df["booked_date"].dtype)
    # df.drop(columns="booked_date", inplace=True, errors='ignore')
    ## print("After sanitizing datetimes:")
    # display(df.dtypes)
    # display(df.head(2))

    df = df[df["_id"].notnull()]
    # if df.empty:
    #   #  print("⚠️ All rows missing _id, nothing to upsert")
    #     return
    # for col in df.columns:
    #     if df[col].dtype == "object" or "datetime" in str(df[col].dtype):
    #         bad = df[col][df[col].apply(lambda x: isinstance(x, pd._libs.tslibs.nattype.NaTType))]
    #         if not bad.empty:
    #             print(f"⚠️ Column {col} still has NaT values:", bad.head())

    ops = []
    for _, row in df.iterrows():
        doc = row.to_dict()
        _id = doc.pop("_id")
        ops.append(UpdateOne({"_id": _id}, {"$set": doc}, upsert=True))

    result = reporting_db[collection_name].bulk_write(ops, ordered=False)
  #  print(f"✅ Upserted: matched={result.matched_count}, modified={result.modified_count}, upserted={len(result.upserted_ids)}")


# -------------------------------
# MAIN ETL RUN
# -------------------------------
def run_etl(collection_name):
    last_run = get_last_run(collection_name)
    print(f"🔍 Last ETL run: {last_run}")

    # 1. Extract incremental
    if collection_name == "milk_purchases":
        relevant_df = extract_incremental_purchases(live_db, collection_name, last_run)
        # display(relevant_df.head(2))
        print(f"📦 Extracted {len(relevant_df)} new/updated purchases")
    else:
        print(f"No extraction defined for collection: {collection_name}")
        relevant_df = pd.DataFrame()

    # 2. Transform
    if collection_name == "milk_purchases":
        transformed_df = transform_purchases(relevant_df)
        print(f"🔄 Transformed DF")
        prices_df = pd.DataFrame(list(live_db["prices"].find({"status": 1})))
        archived_prices_df = pd.DataFrame(list(live_db["archieved_prices"].find({"status": 1})))

        prices_df["plant"]= prices_df["plant"].apply(to_objectid_safe)
        archived_prices_df["plant"]= archived_prices_df["plant"].apply(to_objectid_safe)
        prices_df["source_type"]= prices_df["source_type"].apply(to_objectid_safe)
        archived_prices_df["source_type"]= archived_prices_df["source_type"].apply(to_objectid_safe)
        prices_df["area_office"]= prices_df["area_office"].apply(to_objectid_safe)
        archived_prices_df["area_office"]= archived_prices_df["area_office"].apply(to_objectid_safe)
        prices_df["supplier"]= prices_df["supplier"].apply(to_objectid_safe)
        archived_prices_df["supplier"]= archived_prices_df["supplier"].apply(to_objectid_safe)
        prices_df["collection_point"]= prices_df["collection_point"].apply(to_objectid_safe)
        archived_prices_df["collection_point"]= archived_prices_df["collection_point"].apply(to_objectid_safe)

      #  print("Min/Max WEF in prices:", prices_df["wef"].min(), prices_df["wef"].max())
      #  print("Sample WEF values:", prices_df["wef"].sort_values().head(10).tolist())
      #  print("Sample booked times:", transformed_df["booked_at"].sort_values().head(10).tolist())
      #  print("Min/Max booked_at in purchases:", transformed_df["booked_at"].min(), transformed_df["booked_at"].max())
        ## print("example prices:")
        # display(prices_df.head(2))
        # display(archived_prices_df.head(2))
        transformed_df = attach_prices(transformed_df, prices_df, archived_prices_df)
      #  print("💰 Prices attached")
      #  print(f"No transformation defined for collection: {collection_name}")
    # 3. Load
    ## print("Before Loading DF: ")
    # display(transformed_df.head(2))
    # display(transformed_df.dtypes)
    load_to_reporting(transformed_df, "fact_"+collection_name)

    # 4. Update checkpoint
    if not relevant_df.empty:
        # relevant_df["created_at"] = pd.to_datetime(relevant_df["created_at"], errors="coerce")
        # relevant_df["booked_date"] = pd.to_datetime(relevant_df["booked_date"], errors="coerce")
        new_last_run = relevant_df["created_at"].max()
        update_last_run(new_last_run, collection_name)
        print(f"✅ ETL finished. Updated last_run to {new_last_run}")
    else:     
        print("ℹ️ No new data to process.")


if __name__ == "__main__":
    run_etl("milk_purchases")