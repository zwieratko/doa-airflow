from airflow.hooks.base import BaseHook
import boto3


def get_minio():
    connection = BaseHook.get_connection("openminios3")
    minio = boto3.resource(
        "s3",
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
    )
    return minio
