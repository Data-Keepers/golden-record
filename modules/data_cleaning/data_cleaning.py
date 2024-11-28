import logging

import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting to clean data...")
    # удаляем строки с цифрами
    df = df[~df['client_first_name'].astype(str).str.contains(r'[0-9]', regex=True)]
    df = df[~df['client_last_name'].astype(str).str.contains(r'[0-9]', regex=True)]
    df = df[~df['client_middle_name'].astype(str).str.contains(r'[0-9]', regex=True)]
    return df