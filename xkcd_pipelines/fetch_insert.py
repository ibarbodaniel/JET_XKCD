import logging
import os
from datetime import datetime

import psycopg2
import requests
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

# Database configuration using environment variables
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "default_dbname"),
    "user": os.getenv("DB_USER", "default_user"),
    "password": os.getenv("DB_PASSWORD", "default_password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}


class XKCDPipeline:
    def __init__(self):
        self.db_config = DB_CONFIG

    def main(self):
        try:
            self.handle_table()
        except Exception as e:
            logging.error(f"Error in main: {e}")

    def fetch_comic(self, comic_id):
        url = f"https://xkcd.com/{comic_id}/info.0.json"
        try:
            logging.info(f"Fetching XKCD comic {comic_id}")
            response = requests.get(url)
            response.raise_for_status()
            logging.info(f"Fetching XKCD comic {comic_id} - Success.")
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Fetching XKCD comic {comic_id} - Error: {e}")
            return None

    def does_table_exist(self):
        try:
            logging.info("Checking if the table 'xkcd.comics' exists...")
            query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'xkcd'
                AND table_name = 'comics'
            );
            """
            connection = psycopg2.connect(**self.db_config)
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            connection.close()
            return result[0] if result else False
        except Exception as e:
            logging.error(f"Error checking if the table exists: {e}")
            return False

    def get_latest_comic_id_from_db(self):
        try:
            logging.info("Getting the latest comic ID from the database")
            query = "SELECT comic_id FROM xkcd.comics ORDER BY comic_id DESC LIMIT 1;"
            connection = psycopg2.connect(**self.db_config)
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            connection.close()
            return result[0] if result else None
        except Exception as error:
            logging.error(
                f"Getting the latest comic ID from the database - Error: {error}"
            )
            return None

    def fetch_new_comics(self, start_id):
        new_comics = []
        comic_id = start_id
        consecutive_404_count = 0
        while True:
            comic_data = self.fetch_comic(comic_id)
            if comic_data is None:
                consecutive_404_count += 1
                if consecutive_404_count >= 3:
                    logging.info(
                        "Encountered 3 consecutive 404 errors. Stopping fetch."
                    )
                    break
                comic_id += 1
                continue

            consecutive_404_count = 0
            new_comics.append(comic_data)
            comic_id += 1

        return new_comics

    def handle_table(self):
        try:
            if self.does_table_exist():
                latest_comic_id = self.get_latest_comic_id_from_db()
                if latest_comic_id is None:
                    logging.error("Failed to retrieve the latest comic ID. Exiting.")
                    return
                logging.info(f"Latest comic ID from database: {latest_comic_id}")
                start_id = latest_comic_id + 1
            else:
                logging.info(
                    "Table does not exist. Importing all comics starting from position 1."
                )
                start_id = 1

            new_comics = self.fetch_new_comics(start_id)

            if new_comics:
                self.insert_comic_into_db(new_comics)
                logging.info("New comics inserted successfully.")
            else:
                logging.info("No new comics to insert.")
        except Exception as e:
            logging.error(f"Error handling table: {e}")

    def create_table(self, cursor):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS comics (
            comic_id INT PRIMARY KEY,
            title TEXT,
            safe_title TEXT,
            transcript TEXT,
            alt TEXT,
            image_url TEXT,
            date DATE,
            news TEXT
        );
        """
        try:
            logging.info("Executing SQL commands to create table...")
            cursor.execute(create_table_query)
            logging.info("Table creation completed.")
        except Exception as e:
            logging.error(f"Error executing SQL commands: {e}")
            raise

    def insert_comic_into_db(self, comic_data):
        try:
            logging.info("Connecting to the database...")
            connection = psycopg2.connect(**self.db_config)
            cursor = connection.cursor()
            logging.info("Successfully connected to the database.")

            cursor.execute("CREATE SCHEMA IF NOT EXISTS xkcd;")
            logging.info("Schema creation completed.")

            cursor.execute("SET search_path TO xkcd;")

            self.create_table(cursor)

            insert_query = """
            INSERT INTO comics (comic_id, title, safe_title, transcript, alt, image_url, date, news)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (comic_id) DO NOTHING;
            """
            data_to_insert = [
                (
                    comic["num"],
                    comic["title"],
                    comic["safe_title"],
                    comic.get("transcript"),
                    comic["alt"],
                    comic["img"],
                    datetime.strptime(
                        f"{comic['year']}-{comic['month']}-{comic['day']}", "%Y-%m-%d"
                    ),
                    comic["news"],
                )
                for comic in comic_data
            ]

            cursor.executemany(insert_query, data_to_insert)
            logging.info(f"Inserted {cursor.rowcount} new comics into the database.")

            connection.commit()
            cursor.close()
            connection.close()
            logging.info("Database connection closed.")
        except Exception as error:
            logging.error(f"Error inserting comic data: {error}")
            raise


if __name__ == "__main__":
    pipeline = XKCDPipeline()
    pipeline.main()
