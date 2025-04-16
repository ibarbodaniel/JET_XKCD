# JET_XKCD

This project, **JET_XKCD**, was made for the case interview for a data engineering position at Just Eat Takeaway.

## Features

- Fetch XKCD comic data using the XKCD API.
- Store comic data in a PostgreSQL database.
- Transform and model data using dbt.
- Perform data quality checks with dbt tests.

## Requirements

- Python 3.10+
- PostgreSQL
- Poetry (for dependency management)
- dbt (for data modeling)

## Setup

1. Clone the repository:
    ```bash
    git clone <repository-url>
    cd JET_XKCD
    ```

2. Install dependencies:
    ```bash
    poetry install
    ```

3. Set up your database environment variables. Create a `.env` file in the root directory with the following content:
    ```
    DB_HOST=<your-database-host>
    DB_PORT=<your-database-port>
    DB_NAME=<your-database-name>
    DB_USER=<your-database-username>
    DB_PASSWORD=<your-database-password>
    ```

4. Run the application:
    ```bash
    poetry run python fetch_insert.py
    ```

5. Initialize and run dbt:
    ```bash
    cd xkcd_dbt_project/xkcd
    dbt run
    dbt test
    ```

## Notes

- Ensure your PostgreSQL database is running and accessible before running the application.
- Replace placeholder values in the `.env` file with your actual database credentials.
- For more details, refer to the project documentation or contact the repository maintainer..
