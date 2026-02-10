import sqlite3
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone

DB_PATH = "timeseries.sqlite"
STATIONS = ["chester", "liverpool", "ironbridge", "farndon", "queens_park"]

def load(db, station_key, days=7):
    conn = sqlite3.connect(db)
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    q = """
        SELECT ts_utc, value
        FROM readings
        WHERE station_key = ?
          AND ts_utc >= ?
        ORDER BY ts_utc ASC;
    """
    df = pd.read_sql_query(q, conn, params=(station_key, cutoff))
    conn.close()
    if df.empty:
        return df
    df["datetime_utc"] = pd.to_datetime(df["ts_utc"], unit="s", utc=True)
    return (df.set_index("datetime_utc")
              .resample("15T").mean()
              .interpolate()
              .reset_index())

fig = go.Figure()
for key in STATIONS:
    df = load(DB_PATH, key, days=7)
    if df.empty:
        continue
    fig.add_trace(go.Scatter(
        x=df["datetime_utc"], y=df["value"], mode="lines", name=key
    ))

fig.update_layout(
    title="Water levels – last 7 days (UTC)",
    xaxis_title="Time (UTC)",
    yaxis_title="Level",
    template="plotly_white",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
)
fig.show()
