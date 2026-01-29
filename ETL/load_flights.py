import pandas as pd
from datetime import time
from db import engine

CSV_PATH = "../data/raw/flight_delay_2019_2023.csv"

def hhmm_to_time(val):
    if pd.isna(val):
        return None
    try:
        val = int(val)
        hours = val // 100
        minutes = val % 100
        if hours >= 24 or minutes >= 60:
            return None
        return time(hour=hours, minute=minutes)
    except Exception:
        return None

# 1. Read CSV
df = pd.read_csv(CSV_PATH)

# 2. Rename
df = df.rename(columns={
    "FL_DATE": "flight_date",
    "AIRLINE_CODE": "airline_code",
    "FL_NUMBER": "flight_number",
    "ORIGIN": "origin_airport",
    "DEST": "destination_airport",
    "CRS_DEP_TIME": "scheduled_dep_time",
    "DEP_TIME": "actual_dep_time",
    "CRS_ARR_TIME": "scheduled_arr_time",
    "ARR_TIME": "actual_arr_time",
    "DEP_DELAY": "dep_delay_minutes",
    "ARR_DELAY": "arr_delay_minutes",
    "CANCELLED": "cancelled",
    "DIVERTED": "diverted"
})

# 3. Select ONLY DB columns
db_columns = [
    "flight_date",
    "airline_code",
    "flight_number",
    "origin_airport",
    "destination_airport",
    "scheduled_dep_time",
    "actual_dep_time",
    "scheduled_arr_time",
    "actual_arr_time",
    "dep_delay_minutes",
    "arr_delay_minutes",
    "cancelled",
    "diverted"
]

df = df[db_columns]

# 4. Type conversions
df["flight_date"] = pd.to_datetime(df["flight_date"]).dt.date
df["cancelled"] = df["cancelled"].fillna(0).astype(bool)
df["diverted"] = df["diverted"].fillna(0).astype(bool)
df["dep_delay_minutes"] = df["dep_delay_minutes"].fillna(0)
df["arr_delay_minutes"] = df["arr_delay_minutes"].fillna(0)

# 5. HHMM → TIME (🔥 THIS FIXES YOUR ERROR)
df["scheduled_dep_time"] = df["scheduled_dep_time"].apply(hhmm_to_time)
df["actual_dep_time"] = df["actual_dep_time"].apply(hhmm_to_time)
df["scheduled_arr_time"] = df["scheduled_arr_time"].apply(hhmm_to_time)
df["actual_arr_time"] = df["actual_arr_time"].apply(hhmm_to_time)

# 6. Load
df.to_sql(
    "flights",
    engine,
    if_exists="append",
    index=False,
    chunksize=10000
)

print("✅ Flights data loaded successfully")
