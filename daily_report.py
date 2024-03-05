import logging
from pathlib import Path
import tempfile
import jinja2

from airflow.decorators import dag, task
# from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
# from pendulum import datetime
# import boto3
# import httpx
import botocore
import pandas as pd
import pendulum
import matplotlib.dates as mdates
from apprise import Apprise

from tasks import is_minio_alive
from helpers import get_minio


logger = logging.getLogger(__file__)
DATASET = "weather.csv"
APPRISE_TOKEN = Variable.get("APPRISE_TOKEN")


@task
def extract_yesterday_data():
    logger.info(">> Extracting yesterday data")

    # download dataset
    # STIAHNI Z MINIO
    minio = get_minio()
    tmpfile = tempfile.mkstemp()[1]
    path = Path(tmpfile)
    logger.info(f"Downloading dataset to file: {path}")
    bucket = minio.Bucket("datasets")
    try:
        bucket.download_file(DATASET, path)

        # cleanup, setup, drop dataframe
        df = pd.read_csv(
            path,
            names=[
                "dt",
                "city",
                "country",
                "temp",
                "hum",
                "press",
                "sunrise",
                "sunset",
                "wind_angle",
                "wind_speed",
            ],
        )
        df["dt"] = pd.to_datetime(df["dt"], unit="s")
        df["sunrise"] = pd.to_datetime(df["sunrise"], unit="s")
        df["sunset"] = pd.to_datetime(df["sunset"], unit="s")
        df.drop_duplicates(inplace=True)
        print(df.info())
        # filter data
        filter_yesterday_and_later = df["dt"] >= pendulum.yesterday(
            "utc").to_date_string()
        filter_not_today = df["dt"] < pendulum.today("utc").to_date_string()
        filter_yesterday = filter_yesterday_and_later & filter_not_today
        result = df.loc[filter_yesterday, :]
        # print("Result data type: ", type(result))
        print(result)
    except botocore.exceptions.ClientError:
        logger.warning(
            "Dataset doesn't exist in bucket. Possible first time upload.")
        raise AirflowException("Dataset doesnt exists in bucket.")

    finally:
        # upratam po sebe
        if path.exists():
            path.unlink()

    return result.to_json()


@task
def create_report(data: str):
    logger.info(">> Creating report")
    print(data)
    df = pd.read_json(data)
    print(df.info())
    path = Path(__file__).parent / 'templates'
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(path),
        autoescape=False
    )

    template = env.get_template('weather.tpl.j2')

    date = pendulum.from_timestamp(df.iloc[0]['dt'] / 1000)
    city = df.iloc[0]['city']
    data = {
        'city': city,
        'date': date.to_date_string(),
        'max_temp': df['temp'].max(),
        'min_temp': df['temp'].min(),
        'avg_temp': df['temp'].mean(),
        'timestamp': pendulum.now('utc').to_iso8601_string()
    }
    final_report = template.render(data)
    print(final_report)

    # UPLOAD REPORT TO MINIO
    # save to temp file
    tmpfile = tempfile.mkstemp()[1]
    path = Path(tmpfile)
    with open(path, "a") as dataset:
        print(final_report, file=dataset)
    # upload to minio
    minio = get_minio()
    bucket = minio.Bucket("reports")
    bucket.upload_file(path, f'{city}.txt')
    # cleanup
    path.unlink()


@task
def create_plot(data: str):
    logger.info(">> Creating plot")
    df = pd.read_json(data, convert_dates=["dt", "sunrise", "sunset"])

    city = df.iloc[0]["city"]
    date = pendulum.from_timestamp(
        df.iloc[0]["dt"].timestamp()).to_date_string()

    ax = df.plot(
        x="dt",
        xlabel="čas (hod)",
        y="temp",
        ylabel="teplota (°C)",
        title=f"Teplota v meste {city}, {date}",
    )
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H"))

    # save to temporary file
    path = Path(tempfile.mkstemp(suffix=".png")[1])
    logger.info(f"Saving temporary file to {path}.")
    ax.figure.savefig(path)

    # upload to minio
    minio = get_minio()
    bucket = minio.Bucket("reports")
    bucket.upload_file(path, f"{city.lower()}.png")

    # cleanup
    path.unlink()


@task
def notify():
    logger.info(">> Sending notification")
    yesterday_date = pendulum.yesterday().to_date_string()
    apprise = Apprise()
    apprise.add(f"pbul://{APPRISE_TOKEN}")
    apprise.notify(title="Upozornenie",
                   body=f"Nový report za včerajšok {yesterday_date} je pripravený.")


@dag(
    "daily_report",
    description="Daily report about the scrape weather from openweathermap.com",
    schedule="5 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=["weather", "devops", "telekom", "report"],
    catchup=False,
)
def main():
    # [ is_minio_alive ] -> [ extract_yesterday_data ] -> [ create_report ]
    data = is_minio_alive() >> extract_yesterday_data()
    [create_report(data), create_plot(data)] >> notify()
    # pass


main()
