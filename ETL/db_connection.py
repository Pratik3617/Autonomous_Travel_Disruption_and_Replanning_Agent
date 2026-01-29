from sqlalchemy import create_engine

DB_URL = "postgresql://travel_user:travel_pass@localhost:5432/travel_ai"

engine = create_engine(DB_URL)
