import dash
from dash import dcc, html
import dash_bootstrap_components as dbc

def create_layout():
    return dbc.Container([
        # Hidden dcc.Store components to hold data in the browser's memory
        dcc.Store(id="full-dataset"),
        dcc.Store(id="filtered-dataset"),
        
        html.H2("Milk Procurement Dashboard", className="text-center mt-2 mb-4"),

        # Refresh controls
        dbc.Row([
            dbc.Col(dbc.Button("Manual Refresh", id="btn-refresh", color="primary"), width="auto"),
            dcc.Interval(id="auto-refresh", interval=6*60*1000, n_intervals=0),  # every 6 minutes
        ], className="mb-3"),

        # Filters
        dbc.Row([
            dbc.Col([
                html.Label("Area Office / Plant"),
                dcc.Dropdown(id="filter-area-office", multi=True, placeholder="Select Area Office"),
            ], width=3),
            dbc.Col([
                html.Label("Purchase Type"),
                dcc.Dropdown(id="filter-type", multi=True, placeholder="Select Purchase Type"),
            ], width=3),
            dbc.Col([
                html.Label("Supplier"),
                dcc.Dropdown(id="filter-supplier", multi=True, placeholder="Select Supplier"),
            ], width=3),
            dbc.Col([
                html.Label("Supplier Type"),
                dcc.Dropdown(id="filter-supplier-type", multi=True, placeholder="Select Supplier Type"),
            ], width=3),
        ]),
        
        dbc.Row([
            dbc.Col([
                html.Label("Collection Point"),
                dcc.Dropdown(id="filter-collection-point", multi=True, placeholder="Select Collection Point"),
            ], width=3),
            dbc.Col([
                html.Label("FFL Ownership"),
                dcc.Dropdown(id="filter-is-mcc", options=["Yes", "No"], multi=True, placeholder="Select FFL Status"),
            ], width=3),
        ]),

        dbc.Row([
            dbc.Col([
                html.Label("Date Range"),
                dcc.DatePickerRange(
                    id="filter-date-range",
                    start_date_placeholder_text="Start Date",
                    end_date_placeholder_text="End Date"
                )
            ], width=4),
            dbc.Col([
                html.Label("Volume Type"),
                dcc.RadioItems(
                    id="filter-volume-type",
                    options=[
                        {"label": "Gross Volume", "value": "gross_volume"},
                        {"label": "TS Volume", "value": "ts_volume"},
                        {"label": "Compare Both", "value": "both"},
                    ],
                    value="gross_volume",
                    inline=True
                )
            ], width=4),
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