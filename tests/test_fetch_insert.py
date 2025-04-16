from unittest.mock import MagicMock

import pytest

from xkcd_pipelines.fetch_insert import XKCDPipeline


@pytest.fixture
def pipeline():
    return XKCDPipeline()


def test_does_table_exist_true(mocker, pipeline):
    # Mock the database connection and query result
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = [True]
    mock_connection = mocker.patch("xkcd_pipelines.fetch_insert.psycopg2.connect")
    mock_connection.return_value.cursor.return_value = mock_cursor

    result = pipeline.does_table_exist()
    assert result is True
    mock_cursor.execute.assert_called_once()


def test_does_table_exist_false(mocker, pipeline):
    # Mock the database connection and query result
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = [False]
    mock_connection = mocker.patch("xkcd_pipelines.fetch_insert.psycopg2.connect")
    mock_connection.return_value.cursor.return_value = mock_cursor

    result = pipeline.does_table_exist()
    assert result is False
    mock_cursor.execute.assert_called_once()


def test_get_latest_comic_id_from_db(mocker, pipeline):
    # Mock the database connection and query result
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = [100]
    mock_connection = mocker.patch("xkcd_pipelines.fetch_insert.psycopg2.connect")
    mock_connection.return_value.cursor.return_value = mock_cursor

    result = pipeline.get_latest_comic_id_from_db()
    assert result == 100
    mock_cursor.execute.assert_called_once()


def test_fetch_comic_success(mocker, pipeline):
    # Mock a successful API response
    mock_response = MagicMock()
    mock_response.json.return_value = {"num": 1, "safe_title": "Barrel - Part 1"}
    mock_response.status_code = 200
    mocker.patch("xkcd_pipelines.fetch_insert.requests.get", return_value=mock_response)

    result = pipeline.fetch_comic(1)
    assert result == {"num": 1, "safe_title": "Barrel - Part 1"}


def test_insert_comic_into_db(mocker, pipeline):
    # Mock the database connection
    mock_cursor = MagicMock()
    mock_connection = mocker.patch("xkcd_pipelines.fetch_insert.psycopg2.connect")
    mock_connection.return_value.cursor.return_value = mock_cursor

    comic_data = [
        {
            "num": 1,
            "title": "Title 1",
            "safe_title": "Safe Title 1",
            "transcript": None,
            "alt": "Alt 1",
            "img": "http://example.com/1.png",
            "year": "2025",
            "month": "04",
            "day": "15",
            "news": None,
        }
    ]

    pipeline.insert_comic_into_db(comic_data)
    mock_cursor.executemany.assert_called_once()


def test_is_database_up_to_date_true(mocker, pipeline):
    # Mock the database method to return the latest comic ID
    mocker.patch.object(pipeline, "get_latest_comic_id_from_db", return_value=100)

    # Mock the API method to return the latest comic with the same ID
    mocker.patch.object(pipeline, "fetch_latest_comic", return_value={"num": 100})

    # Assert that the database is up-to-date
    assert pipeline.is_database_up_to_date() is True


def test_is_database_up_to_date_false(mocker, pipeline):
    # Mock the database method to return an outdated comic ID
    mocker.patch.object(pipeline, "get_latest_comic_id_from_db", return_value=99)

    # Mock the API method to return the latest comic with a newer ID
    mocker.patch.object(pipeline, "fetch_latest_comic", return_value={"num": 100})

    # Assert that the database is not up-to-date
    assert pipeline.is_database_up_to_date() is False
