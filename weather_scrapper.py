import logging
from pathlib import Path
import tempfile

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.exceptions import AirflowException
from pendulum import datetime
import httpx

# import boto3
import botocore

from helpers import get_minio


logger = logging.getLogger(__file__)
DATASET = "weather.csv"


@task
def is_service_alive():
    logger.info(">> Testing openweathermap connection")
    try:
        connection = BaseHook.get_connection("openweathermap")
        # url = 'http://apir.openweathermap.org/data/2.5/weather'
        params = {"appid": connection.password}
        response = httpx.get(url=connection.host, params=params)
        if response.status_code == 401:
            logger.error(f"Missing or invalid API key {connection.password}")
            raise AirflowException("Invalid API key.")
        # print(response.status_code)
    except httpx.ConnectError:
        logger.error(f"Invalid hostname {connection.host}")
        raise AirflowException("Invalid host name.")


@task
def is_minio_alive():
    logger.info(">> Testing minio connection")
    try:
        connection = BaseHook.get_connection("openminio")
        # url = "http://18.185.135.59:9001/minio/health/live"
        print(connection.host)
        response = httpx.get(url=connection.host)
        if response.status_code != 200:
            logger.error("Problem to connect to minio!")
            raise AirflowException("Problem to connect to minio!")
    except httpx.ConnectError:
        logger.error(f"Invalid url {connection.host}")
        raise AirflowException("Invalid host name.")


@task
def scrape_data(query: str) -> dict:
    logger.info(">> Scraping data")
    logger.info(type(query))

    connection = BaseHook.get_connection("openweathermap")

    # url = 'http://api.openweathermap.org/data/2.5/weather'
    params = {"q": query, "units": "metric", "appid": connection.password}

    response = httpx.get(url=connection.host, params=params)
    return response.json()


@task
def process_data(data: dict) -> str:
    logger.info(">> Processing data")
    # print(data)

    line = "{},{},{},{},{},{},{},{},{},{}".format(
        data["dt"],
        data["name"],
        data["sys"]["country"],
        data["main"]["temp"],
        data["main"]["humidity"],
        data["main"]["pressure"],
        data["sys"]["sunrise"],
        data["sys"]["sunset"],
        data["wind"]["deg"],
        data["wind"]["speed"],
    )
    # line = f"{data['name']},{data['sys']['country']},{data['dt']},{data['main']['temp']},{data['main']['humidity']},{data['main']['pressure']},{data['sys']['sunrise']},{data['sys']['sunset']},{data['wind']['deg']},{data['wind']['speed']}"

    return line


@task
def publish_data(line: str):
    logger.info(">> Publishing data")
    # print(line)

    # STIAHNI Z MINIO
    minio = get_minio()
    tmpfile = tempfile.mkstemp()[1]
    path = Path(tmpfile)
    logger.info(f"Downloading dataset to file: {path}")
    bucket = minio.Bucket("datasets")
    try:
        bucket.download_file(DATASET, path)
    except botocore.exceptions.ClientError:
        logger.warning("Dataset doesn't exist in bucket. Possible first time upload.")

    # PRIPOJ RIADOK line
    with open(path, "a") as dataset:
        print(line, file=dataset)

    # ODOSLI NA MINIO
    bucket.upload_file(path, DATASET)
    path.unlink()


@dag(
    "weather_scraper",
    description="Scrape weather from openweathermap.com",
    schedule="*/20 * * * *",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    tags=["weather", "devops", "telekom"],
    catchup=False,
    params={
        "query": Param(
            type="string",
            default="humenne,sk",
            title="Query",
            description="Name of the city to get ...",
        )
    },
)
def main():
    data = [is_minio_alive(), is_service_alive()] >> scrape_data("{{ params.query }}")
    processed_data = process_data(data)
    publish_data(processed_data)


main()
