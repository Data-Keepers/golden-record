import logging
import time

import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting to clean data...")
    start_time = time.time()
    # удаляем строки с цифрами
    df = df[~df['client_first_name'].astype(str).str.contains(r'[0-9]', regex=True)]
    df = df[~df['client_last_name'].astype(str).str.contains(r'[0-9]', regex=True)]
    df = df[~df['client_middle_name'].astype(str).str.contains(r'[0-9]', regex=True)]

    logging.info(f"Data cleaning completed, elapsed time: {round(time.time() - start_time, 3)} seconds")
    return df