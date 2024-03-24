import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from udf import download_and_unzip, process_raw_data

with DAG(
        dag_id="migrate_trips",
        start_date=datetime.datetime(2021, 1, 1),
        schedule="@once",
        max_active_runs=1
):
    @task(task_id='download_and_unzip')
    def download_files(urls):
        return download_and_unzip(urls)


    @task(task_id='test_task')
    def process_data(csv_files, ti=None):
        df = process_raw_data(csv_files)

        df_trip = df[
            ['trip_id', 'start_time', 'end_time', 'bikeid', 'tripduration', 'from_station_id', 'to_station_id']].rename(
            columns={'trip_id': 'id'})

        ti.xcom_push(key='trip_data', value=df_trip)

        from_station = df[['from_station_id', 'from_station_name']].rename(
            columns={'from_station_id': 'id', 'from_station_name': 'name'})
        to_station = df[['to_station_id', 'to_station_name']].rename(
            columns={'to_station_id': 'id', 'to_station_name': 'name'})

        df_station = pd.concat([from_station, to_station], ignore_index=True).drop_duplicates()
        ti.xcom_push(key='station_data', value=df_station)


    @task(task_id='to_db')
    def process_trip_table(ti=None):
        from model.models import engine
        trip_data = ti.xcom_pull(key='trip_data')
        trip_data['tripduration'] = trip_data['tripduration'].apply(lambda x: float(x.replace(',', '')) if x else 0)

        last_row = 0
        for next_row in range(0, trip_data.shape[0], 10000):
            if next_row != 0:
                print(f'las_row: {last_row}, nex_row: {next_row}')
                trip_data[last_row:next_row].to_sql(name='trip', con=engine, index=False, if_exists='replace')
                last_row = next_row
                print('Updated 10000 records')


    download_uris = [
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip"
    ]

    task_1 = download_files(download_uris)
    task_2 = process_data(task_1)
    task_3 = process_trip_table()
    task_1 >> task_2 >> task_3
