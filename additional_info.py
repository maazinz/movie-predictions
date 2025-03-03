import sqlite3
import os
import time
import requests
import multiprocessing
import json 

API = os.getenv('TMDB_API')
DB_PATH = 'movies_db.db'

# Function to fetch additional movie information and append to the database
def fetch_info(movie_id):
    cast_url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={API}'
    rating_url = f'https://api.themoviedb.org/3/movie/{movie_id}/release_dates?api_key={API}'

    response = requests.get(cast_url)
    response_rating = requests.get(rating_url)
    data = response.json()
    data_rating = (response_rating.json()).get('results', [])

    age_rating = "NR"
    for country in data_rating:
        if country['iso_3166_1'] == "US":
            certifications = [entry['certification'] for entry in country['release_dates'] if entry['certification']]
            age_rating = ', '.join(certifications) if certifications else "NR"
            break

    cast = data.get('cast', [])
    crew = data.get('crew', [])
    directors = [member for member in crew if member.get('job') == 'Director']
    producers = [member for member in crew if member.get('job') == 'Producer']

    # Sort the cast by popularity in descending order
    sorted_cast = sorted(cast, key=lambda x: x.get('popularity', 0), reverse=True)
    top_director = max(directors, key=lambda x: x.get('popularity', 0), default={})
    top_producer = max(producers, key=lambda x: x.get('popularity', 0), default={})  

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        if top_director:
            director_id = top_director.get('id') if top_director.get('id') else None
            director_name = top_director.get('name') if top_director.get('name') else None
            director_job = top_director.get('job') if top_director.get('job') else None
            cursor.execute("INSERT OR IGNORE INTO crew (crew_id, name, job) VALUES (?, ?, ?)", (director_id, director_name, director_job))
            cursor.execute("UPDATE movies SET director = ? WHERE id = ?", (director_id, movie_id))

        if top_producer:
            producer_id = top_producer.get('id') if top_producer.get('id') else None
            producer_name = top_producer.get('name') if top_producer.get('name') else None
            producer_job = top_producer.get('job') if top_producer.get('job') else None
            cursor.execute("INSERT OR IGNORE INTO crew (crew_id, name, job) VALUES (?, ?, ?)", (producer_id, producer_name, producer_job))
            cursor.execute("UPDATE movies SET producer = ? WHERE id = ?", (producer_id, movie_id))
        
        actor_ids = [str(member.get('id')) for member in sorted_cast[:3] if member.get('id')]
        actor_ids_str = ','.join(actor_ids)
        cursor.execute("UPDATE movies SET actors = ? WHERE id = ?", (actor_ids_str, movie_id))

        for member in sorted_cast[:3]:
            id = member.get('id') if member.get('id') else None
            name = member.get('name') if member.get('name') else None
            popularity = member.get('popularity') if member.get('popularity') else None
            cursor.execute("INSERT OR IGNORE INTO actors (actor_id, actor_name, popularity) VALUES (?, ?, ?)", (id, name, popularity))

        # Insert Age Rating into Movies Table            
        cursor.execute("UPDATE movies SET age_rating = ? WHERE id = ?", (age_rating, movie_id))
        conn.commit()

    print(f"INFO FETCHED FOR MOVIE ID: {movie_id}")
    with open('data/processed_movies.txt', 'a') as file:
        file.write(f"{movie_id}\n")

    time.sleep(0.5)

def get_unprocessed_ids():
    with open('data/processed_movies.txt', 'r') as file:
        processed_ids = [int(line.strip()) for line in file if line.strip().isdigit()]
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM movies")
        a_movie_ids = [mid[0] for mid in cursor.fetchall()]

        unprocessed_ids = list(set(a_movie_ids) - set(processed_ids))
    
    print(f"PROCESSED MOVIES: {len(processed_ids)}, UNPROCESSED MOVIES: {len(unprocessed_ids)}")
    return unprocessed_ids


if __name__ == '__main__':
    movie_ids = get_unprocessed_ids()
    with multiprocessing.Pool(processes=15) as pool:
        pool.map(fetch_info, movie_ids)
        