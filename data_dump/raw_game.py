import os
import logging

import requests
from dotenv import load_dotenv

from db import Database

load_dotenv()
logging.basicConfig(filename='data_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


def get_raw_games(limit: int = 500, offset: int = 0) -> list:
    url = f'{os.environ["DATA_ENDPOINT"]}/games'
    request_body = f"""
        fields *;
        limit {limit};
        offset {offset};
    """

    try:
        response = requests.post(url, data=request_body, headers={"x-api-key": os.environ['AWS_API_KEY']})
        if response.status_code != 200:
            raise Exception(f'Error: {response.status_code}, {response.text}, limit: {limit}, offset: {offset}')
        
        response = response.json()

        if len(response) == 0:
            print(f'No more games to fetch: limit: {limit}, offset: {offset}')
            return []
    except Exception as e:
        logging.error(str(e))
    

    raw_game_data = []
    for game in response:
        try:
            data = (
                game.get('id', -1),
                game.get('name', ''),
                game.get('genres', []),
                game.get('slug', ''),
                game.get('summary', ''),
                game.get('age_ratings', []),
                game.get('artworks', []),
                game.get('category', -1),
                game.get('cover', -1),
                game.get('created_at', -1),
                game.get('external_games', []),
                game.get('first_release_date', -1),
                game.get('game_engines', []),
                game.get('game_modes', []),
                game.get('involved_companies', []),
                game.get('multiplayer_modes', []),
                game.get('platforms', []),
                game.get('release_dates', []),
                game.get('screenshots', []),
                game.get('similar_games', []),
                game.get('tags', []),
                game.get('themes', []),
                game.get('updated_at', -1),
                game.get('url', ''),
                game.get('websites', []),
                game.get('checksum', ''),
                game.get('language_supports', [])
            )

            raw_game_data.append(data)
        except Exception as e:
            logging.error(str(e) + "id: " + str(game['id']))
            continue
    
    return raw_game_data

if __name__ == '__main__':
    db = Database()
    offset = 147000
    limit = 500

    while True:
        print(f'Fetching games: limit: {limit}, offset: {offset}')

        raw_game_data = get_raw_games(limit=limit, offset=offset)
        offset += limit

        if len(raw_game_data) == 0:
            break
        
        try:
            db.add_raw_game(raw_game_data)
        except Exception as e:
            logging.error(str(e))
            continue
