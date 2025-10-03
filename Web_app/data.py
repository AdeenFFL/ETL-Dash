from pymongo import MongoClient
import numpy as np
import pandas as pd

def get_purchase_data():
    reporting_client= MongoClient("mongodb://localhost:27017/")
    reporting_db= reporting_client["staging_db"]
    collection= reporting_db["fact_milk_purchases"]
    
    cursor =collection.find(
        {},
        {
            "_id": 1,
            "area_office_name":1,
            "supplier_type_name": 1,
            "is_mcc":1, 
            "source": 1, 
            "serial_number": 1,
            "supplier_name": 1,
            "ts_volume": 1,
            "gross_volume": 1,
            "plant_id": 1,
            "price": 1,
            "type": 1,
            "booked_at": 1,
            "time": 1,
            "latitude": 1,
            "longitude": 1,
            "code":1
        }
    )
    df = pd.DataFrame(list(cursor))

    if "booked_at" in df.columns:
        df["booked_at"] = pd.to_datetime(df["booked_at"], errors="coerce")
    
    return df
