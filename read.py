import json

movies = []
with open('data/movies.json', 'r', encoding='utf-8') as file:
    for line in file:
        try:
            # Parse each line as a JSON object
            movie = json.loads(line.strip())
            movies.append(movie['id'])
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON on line: {line}")
            print(f"Error message: {e}")

with open('data/movie_ids.json', 'w', encoding='utf-8') as file:
    json.dump(movies, file, indent=4)