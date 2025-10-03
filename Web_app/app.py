# app.py
import dash
from dash import html, dcc, dash_table, Output, Input
import dash_bootstrap_components as dbc

# Initialize the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server 

refresh_interval = dcc.Interval(
    id= "interval-component",
    interval= 10*60*1000, # in milliseconds (10 minutes)
    n_intervals=0
)
heading = dbc.Col(
    [
        html.H1("Milk Purchase Dashboard", style={"textAlign": "center", "marginTop": 20, "marginBottom": 20}),
        refresh_interval,
        html.Div(id="last-update", style={"textAlign": "center", "marginBottom": 10}),
        dcc.Store(id="last-update-store"),
        dbc.Row(
            [
                dbc.Col(
                    html.Div(id="last-update-text", children="Last updated: --", style={"textAlign": "center", "fontSize": "14px"}),
                    width=8
                ),
                dbc.Col(
                    dbc.Button("Refresh now", id="refresh-button", color="primary", n_clicks=0),
                    width=4,
                    style={"textAlign": "right"}
                ),
            ],
            align="center",
        ),
        html.Hr(),
    ],
    width=12,
)

KPIs = dbc.Col(
    [
        html.Div(id="kpi-cards"),
        html.H3("Milk Purchases Data"),
        html.Div(id="table-container"),
    ],
    width=12,
)
   
# # Sidebar layout (filters)
# sidebar = dbc.Col(
#     [
#         html.H2("Filters", className="display-6"),
#         html.Hr(),

#         html.Label("Area Office"),
#         dcc.Dropdown(id="area-office-dropdown", options=[], multi=True, placeholder="Select area office"),

#         html.Br(),
#         html.Label("Purchase Type"),
#         dcc.Dropdown(id="purchase-type-dropdown", options=[], multi=True, placeholder="Select purchase type"),

#         html.Br(),
#         html.Label("Date Range"),
#         dcc.DatePickerRange(id="date-range-picker"),

#         html.Br(), html.Br(),
#         dbc.Button("Refresh Data", id="refresh-button", color="primary", className="w-100")    
# ],

#     width=3,  # 3/12 width of page
#     style={"backgroundColor": "#f8f9fa", "padding": "20px"}
# )

# # Main content layout
# content = dbc.Col(
#     [
#         dbc.Row(
#             [
#                 dbc.Col(dbc.Card(dbc.CardBody([html.H4("Total Litres"), html.H2("0")])), width=3),
#                 dbc.Col(dbc.Card(dbc.CardBody([html.H4("Total Suppliers"), html.H2("0")])), width=3),
#                 dbc.Col(dbc.Card(dbc.CardBody([html.H4("Avg Price %"), html.H2("0")])), width=3),
#             ],
#             className="mb-4"
#         ),
#         dbc.Row(
#             [
#                 dbc.Col(
#                     dbc.Card(
#                         dbc.CardBody([
#                             html.Div(
#                                 [
#                                     html.H6("Total TS Volume:", style={"margin": 1}),
#                                     html.H6("12345", style={"margin": 4, "fontWeight": "bold"})
#                                 ],
#                                 style={"display": "flex", "justifyContent": "flex-start", "alignItems": "center"}
#                             )
#                         ])
#                     )
#                 ),

#                 dbc.Col(
#                     dbc.Card(
#                         dbc.CardBody([
#                             html.Div(
#                                 [
#                                     html.H6("Total Gross Volume:", style={"margin": 1}),
#                                     html.H6("6789", style={"margin": 4, "fontWeight": "bold"})
#                                 ],
#                                 style={"display": "flex", "justifyContent": "flex-start", "alignItems": "center"}
#                             )
#                         ])
#                     )
#                 ),

#                 dbc.Col(
#                     dbc.Card(
#                         dbc.CardBody([
#                             html.Div(
#                                 [
#                                     html.H6("Avg Price %:", style={"margin": 1}),
#                                     html.H6("12.34%", style={"margin": 4, "fontWeight": "bold"})
#                                 ],
#                                 style={"display": "flex", "justifyContent": "flex-start", "alignItems": "center"}
#                             )
#                         ])
#                     )
#                 ),
#             ],
#             className="mb-5"
#         ),

#         dbc.Row(
#             [
#                 dbc.Col(
#                     dcc.Graph(id="map-graph", figure={}),
#                     width=12
#                 )
#             ]
#         ),
#     ],
#     width=9,  # main area
#     style={"padding": "20px"}
# )

# App layout combines sidebar + content
app.layout = dbc.Container(
    dbc.Row([heading, KPIs]),
    fluid=True
)

# Run the app
if __name__ == "__main__":
    app.run(debug=True)
    
