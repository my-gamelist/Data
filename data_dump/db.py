import os
import logging

import psycopg2
from dotenv import load_dotenv

load_dotenv()

class Database:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ['HOST'],
            database=os.environ['DATABASE_NAME'],
            user=os.environ['DATABASE_USER'],
            password=os.environ['DATABASE_PASSWORD'],
            port=5432
        )

        self.cursor = self.conn.cursor()
        self.conn.autocommit = True

        logging.basicConfig(filename='db_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
        print('\n*************************')
        print('* Connected to database *')
        print('*************************\n')
    
    def add_raw_game(self, games: list):
        for game in games:
            self.cursor.execute("""INSERT INTO raw_game 
                (id, name, genres, slug, summary, age_ratings,
                artworks, category, cover, created_at, external_games, 
                first_release_date, game_engines, game_modes, 
                involved_companies, multiplayer_modes, platforms,
                release_dates, screenshots, similar_games, tags, themes,
                updated_at, url, websites, checksum, language_supports) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s)""", game)

            self.conn.commit()

    def add_game(self, games: list):
        for game in games:
            self.cursor.execute("""INSERT INTO game 
                (id, name, category, cover, created_at, first_release_date,
                slug, summary, updated_at, url, checksum) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s)""", game)

            self.conn.commit()

    def get_all_raw_games(self):
        self.cursor.execute('SELECT * FROM raw_game')
        return self.cursor.fetchall()

    def get_columns(self, table: str) -> list:
        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
        """

        self.cursor.execute(query)
        return self.cursor.fetchall()
        
    def __del__(self):
        print('\n******************************')
        print('* Disconnected from database *')
        print('******************************\n')
        logging.shutdown()
        self.conn.close()

if __name__ == '__main__':
    db = Database()