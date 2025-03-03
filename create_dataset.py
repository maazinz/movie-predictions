import json
import sqlite3
import requests
import os
import time
import multiprocessing

# Constants
BASE_URL = "https://api.themoviedb.org/3/movie/{}?language=en-US"
DB_PATH = 'movies_db.db'
BATCH_SIZE = 50
SLEEP_TIME = 2  # To avoid hitting rate limits

# TMDB API Access Token
ACCESS_TOKEN = os.environ.get('TMDB_ACCESS_TOKEN')
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

def fetch_and_insert_movie(movie_id):
    """
    Function to fetch movie data from TMDB API and insert into SQLite database
    """
    url = BASE_URL.format(movie_id)
    response = requests.get(url, headers=HEADERS)
    movie = response.json()
    print(f"PROCESS: {os.getpid()}, {url}")

    # Whether or not the movie is English, if we have seen the movie_id before, we will mark it as processed 
    with sqlite3.connect(DB_PATH) as conn:
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR IGNORE INTO processed_movies (movie_id) VALUES (?)", (movie_id,))
            conn.commit()
        except sqlite3.IntegrityError as e:
            print(f"Error inserting movie with ID: {movie_id}")
            print(f"Error message: {e}")

    try:
        if movie['original_language'] == 'en':
            movie_data = (
                movie_id if movie_id else None,
                movie['title'] if movie['title'] else None,
                movie['budget'] if movie['budget'] else None,
                movie['popularity'] if movie['popularity'] else None,
                movie['revenue'] if movie['revenue'] else None,
                movie['release_date'] if movie['release_date'] else None,
                movie['runtime'] if movie['runtime'] else None,
                ", ".join([company['name'] for company in movie['production_companies']]) if movie['production_companies'] else None,
                ", ".join([country['name'] for country in movie['production_countries']]) if movie['production_countries'] else None,
                ", ".join([country for country in movie['origin_country']]) if movie['origin_country'] else None,
                ", ".join([genre['name'] for genre in movie['genres']]) if movie['genres'] else None,
                movie['vote_average'] if movie['vote_average'] else None,
                movie['vote_count'] if movie['vote_count'] else None,
                ", ".join([language['name'] for language in movie['spoken_languages']]) if movie['spoken_languages'] else None
            )

            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            try:
                cursor.execute("""
                    INSERT INTO movies (
                        id, title, budget, popularity, revenue, release_date, runtime, production_companies, 
                        production_countries, origin_country, genres, vote_average, vote_count, spoken_languages
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, movie_data)

                conn.commit()

                print(f"Inserted movie with ID: {movie_id}")
            except sqlite3.IntegrityError as e:
                print(f"Error inserting movie with ID: {movie_id}")
                print(f"Error message: {e}")
            finally:
                conn.close()

    except KeyError:
        print(f"Error processing movie with ID: {movie_id}")

    time.sleep(SLEEP_TIME)


def get_unprocessed_ids(ids):
    """ 
    Get list of movie IDs that have not been processed yet
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT movie_id FROM processed_movies")
        processed_ids = {row[0] for row in cursor.fetchall()}

    return [mid for mid in ids if mid not in processed_ids]

if __name__ == "__main__":
    with open('data/movie_ids.json', 'r', encoding='utf-8') as file:
        movie_ids = json.load(file)
    
    unprocessed_ids = get_unprocessed_ids(movie_ids)
    print(f"TOTAL LENGTH: {len(movie_ids)}, UNPROCESSED LENGTH: {len(unprocessed_ids)}")

    with multiprocessing.Pool(processes=15) as pool:
        pool.map(fetch_and_insert_movie, unprocessed_ids)
