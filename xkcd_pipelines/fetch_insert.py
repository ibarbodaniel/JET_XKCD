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
            if self.does_table_exist():
                self.handle_existing_table()
            else:
                self.handle_non_existing_table()
        except Exception as e:
            logging.error(f"Error in main: {e}")

    def _fetch_data(self, url, log_message):
        try:
            logging.info(log_message)
            response = requests.get(url)
            response.raise_for_status()
            logging.info(f"{log_message} - Success.")
            return response.json()
        except requests.RequestException as e:
            logging.error(f"{log_message} - Error: {e}")
            return None

    def fetch_comic(self, comic_id):
        url = f"https://xkcd.com/{comic_id}/info.0.json"
        return self._fetch_data(url, f"Fetching XKCD comic {comic_id}")

    def fetch_latest_comic(self):
        url = "https://xkcd.com/info.0.json"
        return self._fetch_data(url, "Fetching the latest XKCD comic")

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

    def get_latest_comic_safe_title_from_db(self):
        try:
            logging.info("Getting the latest comic safe title from the database")
            query = "SELECT safe_title FROM xkcd.comics ORDER BY comic_id ASC LIMIT 1;"
            connection = psycopg2.connect(**self.db_config)
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            connection.close()
            return result[0] if result else None
        except Exception as error:
            logging.error(
                f"Getting the latest comic safe title from the database - Error: {error}"
            )
            return None

    def find_comic_position_by_safe_title(self, safe_title):
        comic_id = 1
        consecutive_404_count = 0
        while True:
            comic_data = self.fetch_comic(comic_id)
            if comic_data is None:
                consecutive_404_count += 1
                if consecutive_404_count >= 3:
                    logging.error(
                        "Encountered 3 consecutive 404 errors. Stopping search."
                    )
                    break
                comic_id += 1
                continue
            consecutive_404_count = 0
            if comic_data["safe_title"] == safe_title:
                logging.info(
                    f"Found comic with safe title '{safe_title}' at position {comic_id}."
                )
                return comic_id
            comic_id += 1
        logging.error(f"Comic with safe title '{safe_title}' not found.")
        return None

    def fetch_new_comics(self, safe_title):
        new_comics = []
        if safe_title is None:
            # Table does not exist: Fetch all comics incrementally starting from position 1
            logging.info("Fetching all comics starting from position 1...")
            comic_id = 1
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
        else:
            # Table exists: Fetch comics in reverse order starting from the position before the latest safe title
            comic_id = self.find_comic_position_by_safe_title(safe_title)
            if comic_id is None:
                logging.error(
                    "Failed to determine the starting comic ID. Exiting fetch."
                )
                return []

            logging.info(
                f"Fetching comics in reverse order starting from position {comic_id - 1}..."
            )
            comic_id -= 1
            consecutive_404_count = 0
            while comic_id > 0:
                comic_data = self.fetch_comic(comic_id)
                if comic_data is None:
                    consecutive_404_count += 1
                    if consecutive_404_count >= 3:
                        logging.info(
                            "Encountered 3 consecutive 404 errors. Stopping fetch."
                        )
                        break
                    comic_id -= 1
                    continue

                consecutive_404_count = 0
                new_comics.append(comic_data)
                comic_id -= 1

        return new_comics

    def handle_existing_table(self):
        try:
            latest_safe_title = self.get_latest_comic_safe_title_from_db()
            if not latest_safe_title:
                logging.error("Failed to retrieve the latest safe title. Exiting.")
                return

            logging.info(f"Latest safe_title from database: {latest_safe_title}")

            new_comics = self.fetch_new_comics(latest_safe_title)

            latest_comic = self.fetch_latest_comic()
            if latest_comic is None:
                logging.error("Failed to fetch the latest comic. Exiting.")
                return

            if latest_comic["num"] not in [comic["num"] for comic in new_comics]:
                new_comics.insert(0, latest_comic)

            if new_comics:
                self.insert_comic_into_db(new_comics)
                logging.info("New comics inserted successfully.")
            else:
                logging.info("No new comics to insert.")
        except Exception as e:
            logging.error(f"Error handling existing table: {e}")

    def handle_non_existing_table(self):
        try:
            logging.info(
                "Table does not exist. Importing all comics starting from position 1."
            )

            latest_comic = self.fetch_latest_comic()
            if latest_comic is None:
                logging.error("Failed to fetch the latest comic. Exiting.")
                return

            self.insert_comic_into_db([latest_comic])
            logging.info("Latest comic inserted successfully.")

            new_comics = self.fetch_new_comics(None)

            if new_comics:
                self.insert_comic_into_db(new_comics)
                logging.info(
                    "All comics starting from position 1 inserted successfully."
                )
            else:
                logging.info("No additional comics to insert.")
        except Exception as e:
            logging.error(f"Error handling non-existing table: {e}")

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
