import sqlite3

conn = sqlite3.connect('movies_db.db')
cursor = conn.cursor()


table_creation_query = """
CREATE TABLE IF NOT EXISTS movies (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    budget REAL,
    popularity REAL,
    revenue REAL,
    release_date TEXT.
    runtime INTEGER,
    production_companies TEXT,
    origin_country TEXT,
    adult BOOLEAN,
    genres TEXT
)
"""