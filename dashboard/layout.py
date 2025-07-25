from dash import dcc, html
import pandas as pd
import plotly.graph_objs as go

# Read precomputed data
df_temp = pd.read_csv("output/temperature_trend.csv")
df_rain = pd.read_csv("output/rainfall_trend.csv")
df_heat = pd.read_csv("output/heatwave_days.csv")


# Convert Kelvin to Celsius
df_temp["avg_temp"] = df_temp["avg_temp"] - 273.15

# Group temperature and rainfall by year (in case duplicates exist)
avg_temp_by_year = df_temp.groupby("year")["avg_temp"].mean().reset_index()
avg_rain_by_year = df_rain.groupby("year")["avg_rainfall"].mean().reset_index()


layout = html.Div(
    style={"backgroundColor": "#f9f9f9", "padding": "40px"},
    children=[
        html.H1(
            "🌏 ClimateScope India Dashboard",
            style={"textAlign": "center", "color": "#222", "fontFamily": "Arial"},
        ),

        html.H2("🌡 Temperature Trend (1990–2020)", style={"color": "#d62728"}),
        dcc.Graph(
            figure={
                "data": [
                    go.Scatter(
                        x=avg_temp_by_year["year"],
                        y=avg_temp_by_year["avg_temp"],
                        mode="lines+markers",
                        line=dict(color="#d62728"),
                        name="Avg Temp (°C)",
                    )
                ],
                "layout": go.Layout(
                    xaxis=dict(title="Year"),
                    yaxis=dict(title="Average Temperature (°C)"),
                    margin={"l": 60, "r": 20, "t": 30, "b": 60},
                    template="plotly_white",
                    hovermode="closest",
                ),
            }
        ),

        html.H2("🌧 Rainfall Trend (1990–2020)", style={"color": "#1f77b4"}),
        dcc.Graph(
            figure={
                "data": [
                    go.Scatter(
                        x=avg_rain_by_year["year"],
                        y=avg_rain_by_year["avg_rainfall"],
                        mode="lines+markers",
                        line=dict(color="#1f77b4"),
                        name="Avg Rainfall (mm/day)",
                    )
                ],
                "layout": go.Layout(
                    xaxis=dict(title="Year"),
                    yaxis=dict(title="Average Rainfall (mm/day)"),
                    margin={"l": 60, "r": 20, "t": 30, "b": 60},
                    template="plotly_white",
                    hovermode="closest",
                ),
            }
        ),

        html.H2("🔥 Heatwave Events Per Year", style={"color": "#ff7f0e"}),
        dcc.Graph(
            figure={
                "data": [
                    go.Bar(
                        x=df_heat["year"],
                        y=df_heat["heatwave_days"],
                        marker=dict(color="#ff7f0e"),
                        name="Heatwave Days",
                    )
                ],
                "layout": go.Layout(
                    xaxis=dict(title="Year"),
                    yaxis=dict(title="Number of Heatwave Days"),
                    margin={"l": 60, "r": 20, "t": 30, "b": 60},
                    template="plotly_white",
                    hovermode="closest",
                ),
            }
        ),
    ]
)
