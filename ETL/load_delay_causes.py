import pandas as pd
from db import engine

CSV_PATH = "../data/raw/delay_causes_bts.csv"

# 1️⃣ Load CSV
df = pd.read_csv(CSV_PATH)

# 2️⃣ Keep only columns we need
df = df[[
    "year",
    "month",
    "carrier",
    "airport",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay"
]]

# 3️⃣ Ensure numeric types (very important)
delay_cols = [
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay"
]

df[delay_cols] = df[delay_cols].fillna(0).astype(float)

# 4️⃣ Remove rows with NO delays at all (key fix)
df["total_delay"] = df[delay_cols].sum(axis=1)
df = df[df["total_delay"] > 0]

# 5️⃣ Compute percentage contribution
df["carrier_delay_pct"] = df["carrier_delay"] / df["total_delay"]
df["weather_delay_pct"] = df["weather_delay"] / df["total_delay"]
df["nas_delay_pct"] = df["nas_delay"] / df["total_delay"]
df["security_delay_pct"] = df["security_delay"] / df["total_delay"]
df["late_aircraft_pct"] = df["late_aircraft_delay"] / df["total_delay"]

# 6️⃣ Final dataframe matching PostgreSQL table EXACTLY
df_final = df[[
    "carrier",
    "airport",
    "year",
    "month",
    "carrier_delay_pct",
    "weather_delay_pct",
    "nas_delay_pct",
    "security_delay_pct",
    "late_aircraft_pct"
]].rename(columns={
    "carrier": "airline_code",
    "airport": "airport_code"
})

# 7️⃣ Load into PostgreSQL
df_final.to_sql(
    "delay_causes",
    engine,
    if_exists="append",
    index=False,
    chunksize=10000
)

print("✅ Delay causes data loaded successfully")
