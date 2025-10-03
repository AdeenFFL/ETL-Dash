import pandas as pd
import json
import dash
from dash import Input, Output, State, dash_table, dcc, html
import plotly.express as px
from app import app
from data import get_purchases_df

# Trigger sources for refresh
refresh_triggers = [
    Input("btn-refresh", "n_clicks"),
    Input("auto-refresh", "n_intervals")
]

# --- Master Callback: Fetches data and stores it ---
@app.callback(
    Output("full-dataset", "data"),
    refresh_triggers
)
def refresh_data(_, __):
    df = get_purchases_df()
    if df.empty:
        return None
    # Convert DataFrame to JSON for storage
    return df.to_json(date_format="iso", orient="split")

# --- Populate Dropdowns after data is loaded ---
@app.callback(
    [
        Output("filter-area-office", "options"),
        Output("filter-type", "options"),
        Output("filter-supplier", "options"),
        Output("filter-supplier-type", "options"),
        Output("filter-collection-point", "options"),
    ],
    Input("full-dataset", "data")
)
def populate_filters(data_json):
    if not data_json:
        return [], [], [], [], []
    
    df = pd.read_json(data_json, orient="split")
    
    area_offices = [{"label": i, "value": i} for i in sorted(df["area_office_name"].dropna().unique())]
    types = [{"label": i, "value": i} for i in sorted(df["type"].dropna().unique())]
    
    # Supplier options based on 'code' for value and 'name - code' for label
    suppliers = [{"label": f"{row['code']} - {row['supplier_name']}", "value": row["code"]}
                 for _, row in df.dropna(subset=["code", "supplier_name"]).drop_duplicates("code").iterrows()]
    
    supplier_types = [{"label": i, "value": i} for i in sorted(df["supplier_type_name"].dropna().unique())]
    collection_points = [{"label": i, "value": i} for i in sorted(df["collection_point_name"].dropna().unique())]

    return area_offices, types, suppliers, supplier_types, collection_points

# --- Callback to apply filters and store filtered data ---
@app.callback(
    Output("filtered-dataset", "data"),
    [
        Input("full-dataset", "data"),
        Input("filter-area-office", "value"),
        Input("filter-type", "value"),
        Input("filter-supplier", "value"),
        Input("filter-supplier-type", "value"),
        Input("filter-collection-point", "value"),
        Input("filter-is-mcc", "value"),
        Input("filter-date-range", "start_date"),
        Input("filter-date-range", "end_date"),
    ]
)
def filter_data(data_json, area_office, ptype, supplier, stype, collection_point, is_mcc, start_date, end_date):
    if not data_json:
        return None

    df = pd.read_json(data_json, orient="split")
    
    # Apply filters
    if area_office:
        df = df[df["area_office_name"].isin(area_office)]
    if ptype:
        df = df[df["type"].isin(ptype)]
    if supplier:
        df = df[df["code"].isin(supplier)]
    if stype:
        df = df[df["supplier_type_name"].isin(stype)]
    if collection_point:
        df = df[df["collection_point_name"].isin(collection_point)]
    if is_mcc:
        df = df[df["is_mcc"].isin(is_mcc)]
    if start_date:
        df = df[df["booked_at"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["booked_at"] <= pd.to_datetime(end_date)]
    
    return df.to_json(date_format="iso", orient="split")

# --- Main Dashboard Callback: Updates all components based on filtered data ---
@app.callback(
    [
        Output("kpi-total-milk", "children"),
        Output("kpi-avg-price", "children"),
        Output("kpi-total-value", "children"),
        Output("kpi-ffl-share", "children"),
        Output("kpi-count-mcc", "children"),
        Output("map-view", "figure"),
        Output("time-series", "figure"),
        Output("bar-area-office", "figure"),
        Output("bar-suppliers", "figure"),
        Output("pie-ffl", "figure"),
        Output("pie-type", "figure"),
        Output("data-table-container", "children"),
    ],
    [
        Input("filtered-dataset", "data"),
        Input("filter-volume-type", "value"),
    ]
)
def update_dashboard(filtered_data_json, volume_type):
    if not filtered_data_json:
        # Return default values for an empty dashboard
        return (["No Data"]*5 +
                [px.Figure()]*6 + 
                [html.Div("No Data to display", className="text-center mt-4")])

    df = pd.read_json(filtered_data_json, orient="split")
    
    # --- KPIs ---
    if volume_type == "gross_volume":
        total_milk = df["gross_volume"].sum()
    elif volume_type == "ts_volume":
        total_milk = df["ts_volume"].sum()
    else:  # both
        total_milk = f"G: {df['gross_volume'].sum():,.0f} | TS: {df['ts_volume'].sum():,.0f}"

    avg_price = df["price"].mean()
    total_value = df["total_value"].sum()
    ffl_share = df["is_mcc"].value_counts(normalize=True).get("Yes", 0) * 100
    unique_mccs = df["collection_point_name"].nunique()

    kpi_total_milk = f"Total Milk: {total_milk:,.0f} L" if isinstance(total_milk, (int, float)) else total_milk
    kpi_avg_price = f"Avg Price: {avg_price:.2f}"
    kpi_total_value = f"Total Value: {total_value:,.0f}"
    kpi_ffl_share = f"FFL %: {ffl_share:.1f}%"
    kpi_count_mcc = f"MCCs/Plants: {unique_mccs}"

    # --- Figures ---
    if df.empty:
        return (kpi_total_milk, kpi_avg_price, kpi_total_value, kpi_ffl_share, kpi_count_mcc,
                px.Figure(), px.Figure(), px.Figure(), px.Figure(), px.Figure(), px.Figure(),
                html.Div("No data for the selected filters.", className="text-center mt-4"))
    
    fig_map = px.scatter_mapbox(df, lat="latitude", lon="longitude",
                                color="area_office_name", hover_name="collection_point_name",
                                size=volume_type if volume_type != "both" else "gross_volume",
                                mapbox_style="carto-positron", zoom=4)

    df_time = df.groupby(df["booked_at"].dt.date).agg({"gross_volume": "sum", "ts_volume": "sum"}).reset_index()
    y_time = [volume_type] if volume_type != "both" else ["gross_volume", "ts_volume"]
    fig_time = px.line(df_time, x="booked_at", y=y_time, title="Milk Purchased Over Time")

    df_area = df.groupby("area_office_name").agg({
        "gross_volume": "sum",
        "ts_volume": "sum"
    }).reset_index()
    fig_bar_area = px.bar(df_area, x="area_office_name", y=volume_type, title="By Area Office")

    df_suppliers = df.groupby("supplier_name").agg({
        "gross_volume": "sum",
        "ts_volume": "sum"
    }).nlargest(10, volume_type if volume_type != "both" else "gross_volume").reset_index()
    fig_bar_suppliers = px.bar(df_suppliers, x="supplier_name", y=volume_type, title="Top 10 Suppliers")
    
    fig_pie_ffl = px.pie(df, names="is_mcc", values=volume_type if volume_type != "both" else "gross_volume",
                         title="FFL vs Non-FFL")
    fig_pie_type = px.pie(df, names="type", values=volume_type if volume_type != "both" else "gross_volume",
                          title="Purchase Type Split")

    # --- Table ---
    # Ensure relevant columns are shown, including name, code, etc.
    cols_to_show = ["booked_at", "supplier_name", "code", "collection_point_name", "gross_volume", "ts_volume", "price", "total_value"]
    table = dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in cols_to_show],
        data=df[cols_to_show].to_dict("records"),
        page_size=20,
        export_format="csv"
    )

    return (kpi_total_milk, kpi_avg_price, kpi_total_value,
            kpi_ffl_share, kpi_count_mcc,
            fig_map, fig_time, fig_bar_area, fig_bar_suppliers,
            fig_pie_ffl, fig_pie_type,
            table)
            
