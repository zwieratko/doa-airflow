import logging

from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
# from pendulum import datetime

import httpx


logger = logging.getLogger(__file__)


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
