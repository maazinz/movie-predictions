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
    release_date TEXT,
    runtime INTEGER,
    production_companies TEXT,
    production_countries TEXT,
    origin_country TEXT,
    adult BOOLEAN,
    genres TEXT,
    vote_average REAL,
    vote_count INTEGER,
    original_language TEXT
)
"""

cursor.execute(table_creation_query)
conn.commit()

conn.close()