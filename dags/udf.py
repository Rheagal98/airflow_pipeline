import pandas as pd
from urllib.request import urlretrieve
import os
import glob
from database import engine
from sqlalchemy.orm import sessionmaker


def download_and_unzip(urls: list) -> list:
    saved_files = []

    # Download file
    for file_num in range(len(urls)):
        save_file_name = urls[file_num].split('/')[-1]
        if urlretrieve(urls[file_num], save_file_name):
            saved_files.append(save_file_name)

    # Extract
    from zipfile import ZipFile
    for save_file in saved_files:
        with ZipFile(save_file, 'r') as zObject:
            # Extracting all the members of the zip
            # into a specific location.
            zObject.extractall()

    # Get all return csv file name
    path = os.getcwd()
    extension = 'csv'
    os.chdir(path)
    if csv_files := glob.glob('*.{}'.format(extension)):
        return csv_files


def process_raw_data(csv_files: list) -> pd.DataFrame:
    # Union all the csv file
    dfs = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)
    return final_df


def ingest_table_from_pandas(table_class, schema, df):
    data_list = df.to_dict(orient='records')

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for row_data in data_list:
            model_validator = schema.model_validate(row_data)
            trip = table_class(**model_validator.model_dump())
            session.merge(trip)

            # Commit periodically to release memory
            if len(session.new) % 1000 == 0:
                session.commit()

        # Commit any remaining changes
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
