import pandas as pd
# import psycopg2
# import glob

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

default_args = {
    'owner': 'bastun'
}

source_file_path = '/home/ubuser/airflow/datasets/tiktok_google_play_reviews.csv'
output_file_path = '/home/ubuser/airflow/tmp/tiktok_google_play_reviews_cleaned.parquet'
cols = ['reviewId', 'userName', 'userImage', 'content', 'score', 'thumbsUpCount',
        'reviewCreatedVersion', 'at', 'replyContent', 'repliedAt']


def determine_branch():
    """Branch DAG to handle empty file case."""
    with open(source_file_path) as f:
        f.readline()  # skip header
        line = f.readline()
        if line == b'':
            return 'empty_file_logging'
        else:
            return 'data_processing'


def read_csv_file():
    """Create pandas dataframe from csv file."""
    df = pd.read_csv(source_file_path, sep=None, names=cols)
    print(df.head())
    return df.to_parquet()


def handle_null_values(ti):
    """Replace NaN values by '-' in the whole df."""
    df = pd.read_parquet(ti.xcom_pull(task_ids='read_csv_file'))
    df = df.fillna('-')
    return df.to_parquet()


def sort_df(ti):
    """Sort dataframe by time ascending."""
    df = pd.read_parquet(ti.xcom_pull(task_ids='handle_null_values'))
    df.sort_values('at', inplace=True)
    return df.to_parquet()


def clean_df_content(ti):
    """Remove from df column 'content' all non-ascii symbols."""
    df = pd.read_parquet(ti.xcom_pull(task_ids='sort_df'))
    df.content = df.content.str.encode('ascii', 'ignore').str.decode('ascii')
    return df.to_parquet(output_file_path)


with DAG(
        dag_id='tiktok_google_play_reviews_dag',
        description='Load and clean tiktok_google_play_reviews data',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['tiktok_google_play_reviews', 'cleansing', 'file sensor'],
) as dag:
    checking_for_file = FileSensor(
        task_id='checking_for_file',
        filepath=source_file_path,
        poke_interval=10,  # настроить
        timeout=60 * 10
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )

    empty_file_logging = BashOperator(
        task_id='empty_file_logging',
        bash_command='echo '
    )

    with TaskGroup('data_processing') as data_processing:
        read_csv_file = PythonOperator(
            task_id='read_csv_file',
            python_callable=read_csv_file
        )

        handle_null_values = PythonOperator(
            task_id='handle_null_values',
            python_callable=handle_null_values
        )

        sort_df = PythonOperator(
            task_id='sort_df',
            python_callable=sort_df
        )

        clean_df_content = PythonOperator(
            task_id='clean_df_content',
            python_callable=clean_df_content,
            outlets=[Dataset("output_file_path")]
        )

    checking_for_file >> determine_branch >> [data_processing, empty_file_logging]
