import sqlite3

conn = sqlite3.connect('movies_db.db')
cursor = conn.cursor()


table_creation_query = [
"""
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
    genres TEXT,
    vote_average REAL,
    vote_count INTEGER,
    spoken_languages TEXT
)
""", 
"""
CREATE TABLE IF NOT EXISTS actors (
    actor_id INTEGER PRIMARY KEY,
    actor_name TEXT,
    popularity REAL
    )
""",
"""
CREATE TABLE IF NOT EXISTS processed_movies (movie_id INTEGER PRIMARY KEY)
""",
"""
CREATE TABLE IF NOT EXISTS crew (
    crew_id INTEGER PRIMARY KEY,
    name TEXT,
    job TEXT)
"""
]

update_table_query = [
"ALTER TABLE movies ADD COLUMN actors TEXT;",
"ALTER TABLE movies ADD COLUMN director TEXT;",
"ALTER TABLE movies ADD COLUMN producer TEXT;",
"ALTER TABLE movies ADD COLUMN age_rating TEXT;"
]

insertion_query = """
INSERT INTO processed_movies (movie_id)
               SELECT id FROM movies
"""

# cursor.execute(table_creation_query[3])
# conn.commit()

# conn.close()

for query in update_table_query:
    cursor.execute(query)
    conn.commit()
conn.close()