from pymongo import MongoClient
import pandas as pd

def get_purchases_df() -> pd.DataFrame:
    """
    Fetches and processes milk purchase data from MongoDB.
    Returns a pandas DataFrame.
    """
    try:
        # Use a context manager for the client
        with MongoClient("mongodb://localhost:27017/") as client:
            db = client["staging_db"]
            purchases_cursor = db["fact_milk_purchases"].find({}, {
                "booked_at": 1,
                "area_office_name": 1,
                "type": 1,
                "collection_point_name": 1,
                "code": 1,
                "supplier_name": 1,
                "is_mcc": 1,
                "serial_number": 1,
                "latitude": 1,
                "longitude": 1,
                "supplier_type_name": 1,
                "gross_volume": 1,
                "price": 1,
                "ts_volume": 1
            })
            df = pd.DataFrame(list(purchases_cursor))

    except Exception as e:
        print(f"Error fetching data from MongoDB: {e}")
        return pd.DataFrame() # Return an empty DataFrame on error

    if not df.empty:
        # Convert booked_at to datetime to allow filtering
        df["booked_at"] = pd.to_datetime(df["booked_at"], errors="coerce")
        # Normalize FFL ownership for consistent filtering
        df["is_mcc"] = df["is_mcc"].apply(
            lambda x: "Yes" if str(x).lower() in ["1", "true", "yes"] else "No"
        )
        # Fill NaNs to prevent calculation errors
        df = df.fillna({"gross_volume": 0, "ts_volume": 0, "price": 0})
        # Compute total value
        df["total_value"] = df["price"] * df["gross_volume"]

    return df