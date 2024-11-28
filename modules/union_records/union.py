import json
import logging
import time
import pandas as pd
from multiprocessing import Pool, cpu_count
from . import valid


def process_group(group):
    """Обрабатывает одну группу кластера."""
    sorted_group = group.sort_values(
        by=["update_date", "create_date"],
        ascending=False
    )
    golden_record = sorted_group.iloc[0].to_dict()
    nan_fields = [key for key, value in golden_record.items() if (
        pd.isna(value) or
        (valid.VALID_RECORDS.get(key) and not valid.VALID_RECORDS.get(key)(str(value)))
    )]
    replaced_ids = {}
    for index, row in sorted_group.iterrows():
        for field in nan_fields:
            if (
                pd.isna(golden_record[field]) or
                (valid.VALID_RECORDS.get(field) and not valid.VALID_RECORDS.get(field)(golden_record[field]))
            ) and not pd.isna(row[field]):
                if valid.VALID_RECORDS.get(field):
                    if valid.VALID_RECORDS.get(field)(str(row[field])):
                        golden_record[field] = row[field]
                        replaced_ids[field] = row["client_id"]
                else:
                    golden_record[field] = row[field]
                    replaced_ids[field] = row["client_id"]
        if len(replaced_ids) == len(nan_fields):
            break
    golden_record["field_taken_from_idx"] = json.dumps(replaced_ids)
    return golden_record


def union_records_by_cluster_id(records: pd.DataFrame) -> [pd.DataFrame, pd.DataFrame]:
    logging.info("Starting to merge similar rows...")
    start_time = time.time()

    # Разделяем данные на уникальные записи и мультиаккаунтные группы
    grouped = records.groupby("cluster_id")
    result_unique_records = grouped.filter(lambda x: len(x) == 1)
    grouped_multiacc = grouped.filter(lambda x: len(x) > 1)

    # Список групп для обработки
    clusters = [group for _, group in grouped_multiacc.groupby("cluster_id")]

    # Используем пул процессов
    with Pool(processes=cpu_count()) as pool:
        result_grouped = pool.map(process_group, clusters)

    logging.info(f"""Row merging completed.
                 Merged rows count: {len(result_grouped)}
                 Unique rows count: {len(result_unique_records)}
                 Elapsed time: {round(time.time() - start_time, 3)} seconds""")

    return pd.DataFrame(result_grouped), result_unique_records
