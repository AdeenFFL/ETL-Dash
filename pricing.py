# -------------------------------
# PRICING LOGIC (with debug logs)
# -------------------------------
import numpy as np
import pandas as pd

def get_plant_base_price(purchase, prices, archived_prices):
    """
    Pricing for purchases linked to plants.
    """
    if purchase.get("serial_number")==427241: 
        print("Debug")
    if purchase.get("price") not in [None, 0, np.nan, "nan", "null"]:
        if purchase.get("price")==427241: 
            print("Debug - returning existing price")

        return purchase["price"]

    supplier_type = purchase.get("supplier_type_id")
    booked_at = purchase.get("booked_at")
    # booked_at = pd.to_datetime(booked_at)
    # print(booked_at)
    # print(f"\n🌱 get_plant_base_price: supplier_type={supplier_type}, booked_at={booked_at}")

    # Active prices
    subset = prices[
        (prices["source_type"] == supplier_type) &
        (prices["wef"] <= booked_at)
              ].sort_values("wef", ascending=False)
    
    if purchase.get("serial_number")==427241:
        print(f"  ➡️ Active prices subset size={len(subset)}"
              )
    # print(f"   ➡️ Active prices subset size={len(subset)}")
    match = subset.loc[subset["supplier"] == purchase["supplier_id"]].head(1)
    if match.empty:
        # print("  ❌ No exact supplier match in active, trying supplier=None")
        match = subset.loc[subset["supplier"].isna()].head(1)

    if purchase.get("serial_number")==427241:
        print(f"  Match subset: {len(match)}")

    if not match.empty:
        # print("   ✅ Plant price found (active)")
        print(match.iloc[0]["price"])
        return match.iloc[0]["price"]

    # Archived fallback
    subset = archived_prices[
        (archived_prices["source_type"] == supplier_type) &
        (archived_prices["wef"] <= booked_at)
    ].sort_values("wef", ascending=False)
    # print(f"   ➡️ Archived prices subset size={len(subset)}")

    match = subset.loc[subset["supplier"] == purchase["supplier_id"]].head(1)
    if match.empty:
        # print("   ❌ No exact supplier match in archived, trying supplier=None")
        match = subset.loc[subset["supplier"].isna()].head(1)

    if not match.empty:
        # print("   ✅ Plant price found (archived)"")
        # print(f"   📅 Using effective WEF={match.iloc[0]['wef']}")
        return match.iloc[0]["price"]

    # print("   ⚠️ No plant price match found, returning 0")
    return -2


def get_base_price(purchase, prices, archived_prices):
    """
    Main pricing function — AO/CP cascade.
    """
    # none_price_count = purchase["price"].isna().sum()
    # print(f"Number of purchases with price=None: {none_price_count}")
    # nan_price_count = purchase["price"].isnull().sum()
    # print(f"Number of purchases with price=NaN: {nan_price_count}")
    # print(purchase["price"].valuecounts())
    # zero_price_count = purchase[purchase["price"]==0].sum()
    # print(f"Number of purchases with price=0: {zero_price_count}")
    # string_price_count = purchase[purchase["price"]=="nan"].sum()
    # print(f"Number of purchases with price='nan': {string_price_count}")

    if purchase.get("price") not in [None, 0, np.nan, "nan"]:
        
        print(f"   ℹ️ Existing price found: {purchase.get('price')} → using it")
        # print(purchase.get("price").dtype)
        return purchase.get("price")

    ao_id = purchase.get("area_office_id_ao")
    # if ao_id is None:
    #     ao_id = purchase.get("area_office_id")
    cp_id = purchase.get("mcc_id")
    booked_at = purchase.get("booked_at")

    # print(f"\n📦 get_base_price for purchase_id={purchase.get('_id')}, "
    #       f"ao_id={ao_id}, cp_id={cp_id}, supplier={purchase.get('supplier_id')}, "
    #       f"type={purchase.get('supplier_type_id')}, time={booked_at}")

    # Plant-based fallback
    if purchase.get("plant_id") is not None and not pd.isna(purchase.get("plant_id")):
        # print("   🔄 Plant-based purchase → fallback to get_plant_base_price")
        return get_plant_base_price(purchase, prices, archived_prices)

    # Active prices
    base_pricing = prices
    subset = base_pricing[
        (base_pricing["wef"] <= booked_at) &
        (base_pricing["area_office"] == ao_id)
    ].sort_values("wef", ascending=False)
    print(f"   ➡️ Active subset size={len(subset)}")

    if subset.empty:
        base_pricing = archived_prices
        subset = base_pricing[
            (base_pricing["wef"] <= booked_at) &
            (base_pricing["area_office"] == ao_id)
        ].sort_values("wef", ascending=False)
        print(f"   ➡️ Archived subset size={len(subset)}")
    
    if subset.empty:
        # print("   ⚠️ No active or archived prices for this AO/time → returning 0")
        return -1

    date = subset.iloc[0]["wef"]
    # print(f"   📅 Using effective WEF={date}")

    # Fallback sequence (most specific → most general)
    for idx, cond in enumerate([
        {"supplier": purchase["supplier_id"], "collection_point": cp_id},
        {"supplier": purchase["supplier_id"], "collection_point": None},
        {"supplier": None, "collection_point": cp_id},
        {"supplier": None, "collection_point": None},
    ]):
        match = base_pricing[
            (base_pricing["area_office"] == ao_id) &
            (base_pricing["source_type"] == purchase["supplier_type_id"]) &
            (
    ((base_pricing["supplier"] == cond["supplier"]) | 
     (base_pricing["supplier"].isna() & (cond["supplier"] is None)))
    ) &
    (
    ((base_pricing["collection_point"] == cond["collection_point"]) | 
     (base_pricing["collection_point"].isna() & (cond["collection_point"] is None)))
    )
     &
            (base_pricing["wef"] == date)
        ]
        # print(f"   🔎 Fallback {idx+1}: cond={cond}, matches={len(match)}")
        if not match.empty:
            # print(f"   ✅ Match found → price={match.iloc[0]['price']}")
            if match.iloc[0]['price'] in [None, np.nan]:
                print("   ⚠️ Matched price is None or NaN → returning 0")
            return match.iloc[0]["price"]

    # print("   ⚠️ No match found in any fallback sequence → returning 0")
    return -3


# def attach_prices(purchases_df, prices_df, archived_prices_df):
#     """
#     Add 'price' column to transformed purchases DataFrame.
#     """
#     prices_df["wef"] = pd.to_datetime(prices_df["wef"], errors="coerce")
#     archived_prices_df["wef"] = pd.to_datetime(archived_prices_df["wef"], errors="coerce")

#     # print("💾 Prices DF sample dtypes:")
#     # print(prices_df.dtypes.head())

#     purchases_df["price"] = purchases_df.apply(
#         lambda row: get_base_price(row.to_dict(), prices_df, archived_prices_df),
#         axis=1
#     )
#     return purchases_df


from tqdm import tqdm
import atexit

def attach_prices(purchases_df, prices_df, archived_prices_df):
    # display(prices_df.dtypes)
    prices_df["wef"] = pd.to_datetime(prices_df["wef"], errors="coerce")
    archived_prices_df["wef"] = pd.to_datetime(archived_prices_df["wef"], errors="coerce")

    print(f"💰 Attaching prices for {len(purchases_df)} purchases...")

    purchases_df["price"] = [
        get_base_price(row.to_dict(), prices_df, archived_prices_df)
        for _, row in tqdm(purchases_df.iterrows(), total=len(purchases_df))
    ]

    print("✅ Pricing complete.")
    return purchases_df
