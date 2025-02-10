# import requests
# import os

# api_key = os.environ.get('TMDB_API')

import requests
import os
# url = "https://api.themoviedb.org/3/trending/movie/day?language=en-US"
# url = "https://api.themoviedb.org/3/movie/top_rated?language=en-US&page=2"
base_url = "https://api.themoviedb.org/3/discover/movie?include_adult=true&include_video=false&language=en-US&page={}"


access_token = os.environ.get('TMDB_ACCESS_TOKEN')
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {access_token}"
}

aggregated_res = []
for page_number in range(1, 3):
    url = base_url.format(page_number)
    print(url)
    response = requests.get(url, headers=headers)
    aggregated_res.append(response.json()['results'])


for page in aggregated_res:
    for movie in page:
        print(movie['title'])
# for title in aggregated_res:
    

# print(response.text)

# for resp in response.json()['results']:
#     print(resp['title'])