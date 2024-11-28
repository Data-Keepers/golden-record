import numpy as np
from transliterate import translit
from multiprocessing import Pool, cpu_count
import pandas as pd


def transliterate_to_cyrillic(text):
    if not isinstance(text, str) or text.strip() == "":
        return text
    try:
        return translit(text, 'ru')
    except:
        return text


def columns_preparing(chunk):
    """Применяет транслитерацию к одной части DataFrame и делает строку прописными буквами."""
    for col in ['client_first_name', 'client_last_name', 'client_middle_name', 'client_fio_full']:
        chunk[col] = chunk[col].str.lower().apply(transliterate_to_cyrillic)
    return chunk


def prepare_data(df: pd.DataFrame, n_processes=None) -> pd.DataFrame:
    n_processes = n_processes or cpu_count()
    chunk_size = len(df) // n_processes
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

    with Pool(processes=n_processes) as pool:
        results = pool.map(columns_preparing, chunks)

    return pd.concat(results)

