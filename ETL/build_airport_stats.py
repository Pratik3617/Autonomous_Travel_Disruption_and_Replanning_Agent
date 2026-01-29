from db import engine
from sqlalchemy import text

QUERY = text(
    """
    INSERT INTO airport_stats
    SELECT
        origin_airport AS airport_code,
        AVG(arr_delay_minutes) AS avg_delay,
        AVG(CASE WHEN cancelled THEN 1 ELSE 0 END) AS cancellation_rate,
        AVG(CASE WHEN arr_delay_minutes > 15 THEN 1 ELSE 0 END) AS weather_risk_score
    FROM flights
    GROUP BY origin_airport;
    """
)

with engine.begin() as conn:
    conn.execute(QUERY)

print("Airport stats built")
