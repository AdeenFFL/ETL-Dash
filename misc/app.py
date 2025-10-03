import dash
from dash import dcc, html, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pymongo import MongoClient
import json

# --- Data Fetching Function ---
def get_purchases_df():
    """
    Fetches and processes milk purchase data from MongoDB.
    Returns a pandas DataFrame.
    """
    try:
        with MongoClient("mongodb://localhost:27017/") as client:
            db = client["staging_db"]
            purchases_cursor = db["fact_milk_purchases"].find({}, {
                "booked_at": 1, "area_office_name": 1, "type": 1, "collection_point_name": 1,
                "code": 1, "supplier_name": 1, "is_mcc": 1, "serial_number": 1,
                "latitude": 1, "longitude": 1, "supplier_type_name": 1,
                "gross_volume": 1, "price": 1, "ts_volume": 1
            })
            df = pd.DataFrame(list(purchases_cursor))
    except Exception as e:
        print(f"Error fetching data from MongoDB: {e}")
        return pd.DataFrame()

    if not df.empty:
        # Explicitly convert columns to numeric types and handle non-numeric values
        df["price"] = pd.to_numeric(df["price"], errors='coerce').astype(float)
        df["gross_volume"] = pd.to_numeric(df["gross_volume"], errors='coerce').astype(float)
        
        # --- FIX FOR OverflowError ---
        # Identify string columns and clean them
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            # Replace non-UTF-8 characters or very long strings
            df[col] = df[col].apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8') if isinstance(x, str) else x)
        
        df["booked_at"] = pd.to_datetime(df["booked_at"], errors="coerce")
        df["is_mcc"] = df["is_mcc"].apply(
            lambda x: "Yes" if str(x).lower() in ["1", "true", "yes"] else "No"
        )
        df = df.fillna({"gross_volume": 0, "ts_volume": 0, "price": 0})
        df["total_value"] = df["price"] * df["gross_volume"]

    return df

# --- App Initialization and Layout ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Milk Procurement Dashboard"
server = app.server

app.layout = dbc.Container([
    dcc.Store(id="full-dataset"),
    dcc.Store(id="filtered-dataset"),
    
    html.H2("Milk Procurement Dashboard", className="text-center mt-2 mb-4"),
    dbc.Row([
        dbc.Col(dbc.Button("Manual Refresh", id="btn-refresh", color="primary"), width="auto"),
        dcc.Interval(id="auto-refresh", interval=6*60*1000, n_intervals=0),
    ], className="mb-3"),

    # Filters
    dbc.Row([
        dbc.Col([html.Label("Area Office / Plant"), dcc.Dropdown(id="filter-area-office", multi=True, placeholder="Select Area Office")], width=3),
        dbc.Col([html.Label("Purchase Type"), dcc.Dropdown(id="filter-type", multi=True, placeholder="Select Purchase Type")], width=3),
        dbc.Col([html.Label("Supplier"), dcc.Dropdown(id="filter-supplier", multi=True, placeholder="Select Supplier")], width=3),
        dbc.Col([html.Label("Supplier Type"), dcc.Dropdown(id="filter-supplier-type", multi=True, placeholder="Select Supplier Type")], width=3),
    ]),
    
    dbc.Row([
        dbc.Col([html.Label("Collection Point"), dcc.Dropdown(id="filter-collection-point", multi=True, placeholder="Select Collection Point")], width=3),
        dbc.Col([html.Label("FFL Ownership"), dcc.Dropdown(id="filter-is-mcc", options=["Yes", "No"], multi=True, placeholder="Select FFL Status")], width=3),
    ]),

    dbc.Row([
        dbc.Col([html.Label("Date Range"), dcc.DatePickerRange(id="filter-date-range", start_date_placeholder_text="Start Date", end_date_placeholder_text="End Date")], width=4),
        dbc.Col([html.Label("Volume Type"), dcc.RadioItems(id="filter-volume-type", options=[{"label": "Gross Volume", "value": "gross_volume"}, {"label": "TS Volume", "value": "ts_volume"}, {"label": "Compare Both", "value": "both"}], value="gross_volume", inline=True)], width=4),
    ], className="mt-3 mb-4"),

    html.Hr(),

    # KPIs
    dbc.Row([
        dbc.Col(html.Div(id="kpi-total-milk", className="p-3 border rounded bg-light"), width=2),
        dbc.Col(html.Div(id="kpi-avg-price", className="p-3 border rounded bg-light"), width=2),
        dbc.Col(html.Div(id="kpi-total-value", className="p-3 border rounded bg-light"), width=2),
        dbc.Col(html.Div(id="kpi-ffl-share", className="p-3 border rounded bg-light"), width=2),
        dbc.Col(html.Div(id="kpi-count-mcc", className="p-3 border rounded bg-light"), width=2),
    ], className="mb-4"),

    html.Hr(),

    # Tabs for charts
    dbc.Tabs([
        dbc.Tab(dcc.Graph(id="map-view"), label="Map View"),
        dbc.Tab(dcc.Graph(id="time-series"), label="Time Series"),
        dbc.Tab(dcc.Graph(id="bar-area-office"), label="By Area Office"),
        dbc.Tab(dcc.Graph(id="bar-suppliers"), label="Top Suppliers"),
        dbc.Tab(dcc.Graph(id="pie-ffl"), label="FFL vs Non-FFL"),
        dbc.Tab(dcc.Graph(id="pie-type"), label="Purchase Type Split"),
        dbc.Tab(html.Div(id="data-table-container"), label="Data Table"),
    ]),
], fluid=True)

# --- Callbacks ---
@app.callback(Output("full-dataset", "data"), Input("btn-refresh", "n_clicks"), Input("auto-refresh", "n_intervals"))
def refresh_data(_, __):
    df = get_purchases_df()
    if df.empty:
        return None
    return df.to_json(date_format="iso", orient="split")

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
    suppliers = [{"label": f"{row['code']} - {row['supplier_name']}", "value": row["code"]} for _, row in df.dropna(subset=["code", "supplier_name"]).drop_duplicates("code").iterrows()]
    supplier_types = [{"label": i, "value": i} for i in sorted(df["supplier_type_name"].dropna().unique())]
    collection_points = [{"label": i, "value": i} for i in sorted(df["collection_point_name"].dropna().unique())]
    return area_offices, types, suppliers, supplier_types, collection_points

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
    df["booked_at"] = pd.to_datetime(df["booked_at"])

    if area_office: df = df[df["area_office_name"].isin(area_office)]
    if ptype: df = df[df["type"].isin(ptype)]
    if supplier: df = df[df["code"].isin(supplier)]
    if stype: df = df[df["supplier_type_name"].isin(stype)]
    if collection_point: df = df[df["collection_point_name"].isin(collection_point)]
    if is_mcc: df = df[df["is_mcc"].isin(is_mcc)]
    if start_date: df = df[df["booked_at"] >= pd.to_datetime(start_date)]
    if end_date: df = df[df["booked_at"] <= pd.to_datetime(end_date)]

    return df.to_json(date_format="iso", orient="split")

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
        return (["No Data"]*5 + [go.Figure()]*6 + [html.Div("No Data to display", className="text-center mt-4")])

    df = pd.read_json(filtered_data_json, orient="split")
    df["booked_at"] = pd.to_datetime(df["booked_at"])
    
    # --- KPIs ---
    if df.empty:
         return (["No Data"]*5 + [go.Figure()]*6 + [html.Div("No data for the selected filters.", className="text-center mt-4")])
    
    if volume_type == "gross_volume":
        total_milk = df["gross_volume"].sum()
    elif volume_type == "ts_volume":
        total_milk = df["ts_volume"].sum()
    else: total_milk = f"G: {df['gross_volume'].sum():,.0f} | TS: {df['ts_volume'].sum():,.0f}"

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
    fig_map = px.scatter_mapbox(df, lat="latitude", lon="longitude", color="area_office_name", hover_name="collection_point_name", size=volume_type if volume_type != "both" else "gross_volume", mapbox_style="carto-positron", zoom=4)
    df_time = df.groupby(df["booked_at"].dt.date).agg({"gross_volume": "sum", "ts_volume": "sum"}).reset_index()
    y_time = [volume_type] if volume_type != "both" else ["gross_volume", "ts_volume"]
    fig_time = px.line(df_time, x="booked_at", y=y_time, title="Milk Purchased Over Time")
    fig_time.update_layout(xaxis_title="Date", yaxis_title="Volume")

    df_area = df.groupby("area_office_name").agg({"gross_volume": "sum", "ts_volume": "sum"}).reset_index()
    fig_bar_area = px.bar(df_area, x="area_office_name", y=volume_type, title="By Area Office")
    fig_bar_area.update_layout(xaxis_title="Area Office", yaxis_title="Volume")

    df_suppliers = df.groupby("supplier_name").agg({"gross_volume": "sum", "ts_volume": "sum"}).nlargest(10, volume_type if volume_type != "both" else "gross_volume").reset_index()
    fig_bar_suppliers = px.bar(df_suppliers, x="supplier_name", y=volume_type, title="Top 10 Suppliers")
    fig_bar_suppliers.update_layout(xaxis_title="Supplier", yaxis_title="Volume")

    fig_pie_ffl = px.pie(df, names="is_mcc", values=volume_type if volume_type != "both" else "gross_volume", title="FFL vs Non-FFL")
    fig_pie_type = px.pie(df, names="type", values=volume_type if volume_type != "both" else "gross_volume", title="Purchase Type Split")

    cols_to_show = ["booked_at", "supplier_name", "code", "collection_point_name", "gross_volume", "ts_volume", "price", "total_value"]
    table = dash_table.DataTable(columns=[{"name": c, "id": c} for c in cols_to_show], data=df[cols_to_show].to_dict("records"), page_size=20, export_format="csv")

    return (kpi_total_milk, kpi_avg_price, kpi_total_value,
            kpi_ffl_share, kpi_count_mcc,
            fig_map, fig_time, fig_bar_area, fig_bar_suppliers,
            fig_pie_ffl, fig_pie_type,
            table)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)