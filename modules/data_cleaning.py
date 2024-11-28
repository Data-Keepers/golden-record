import logging
import time

import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting to clean data...")
    start_time = time.time()

    # Регулярное выражение для разрешённых символов (русские, английские буквы и пробелы)
    allowed_pattern = r'^[a-zA-Zа-яА-ЯёЁ\s]+$'

    df = df[df['client_first_name'].astype(str).str.match(allowed_pattern, na=False)]
    df = df[df['client_last_name'].astype(str).str.match(allowed_pattern, na=False)]
    df = df[df['client_middle_name'].astype(str).str.match(allowed_pattern, na=False)]
    df = df[df['client_fio_full'].astype(str).str.match(allowed_pattern, na=False)]

    logging.info(f"Data cleaning completed, elapsed time: {round(time.time() - start_time, 3)} seconds")
    return df
