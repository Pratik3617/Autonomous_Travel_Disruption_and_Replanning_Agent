from sqlalchemy import text
from db import engine

QUERY = text("""
INSERT INTO route_stats
SELECT
    origin_airport,
    destination_airport,
    airline_code,
    AVG(dep_delay_minutes) AS avg_dep_delay,
    AVG(arr_delay_minutes) AS avg_arr_delay,
    AVG(CASE WHEN cancelled THEN 1 ELSE 0 END) AS cancellation_rate,
    AVG(CASE WHEN arr_delay_minutes > 15 THEN 1 ELSE 0 END) AS delay_probability,
    COUNT(*) AS sample_size
FROM flights
GROUP BY origin_airport, destination_airport, airline_code;
""")

with engine.begin() as conn:
    conn.execute(QUERY)

print("✅ Route stats built successfully")
