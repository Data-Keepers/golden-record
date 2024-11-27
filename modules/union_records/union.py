import json

import pandas as pd


def union_records_by_cluster_id(records: pd.DataFrame) -> [pd.DataFrame, pd.DataFrame]:
    grouped = records.groupby("cluster_id")
    result_unique_records = grouped.filter(lambda x: len(x) == 1)
    result_grouped = []
    grouped_multiacc = grouped.filter(lambda x: len(x) > 1)
    for cluster, group in grouped_multiacc.groupby("cluster_id"):
        sorted_group = group.sort_values(
            by=["update_date", "create_date"],
            ascending=False
            # na_position="last"
        )
        golden_record = sorted_group.iloc[0].to_dict()
        nan_fields = [key for key, value in golden_record.items() if pd.isna(value)]
        replaced_ids = {}
        for index, row in sorted_group.iterrows():
            for field in nan_fields:
                if pd.isna(golden_record[field]) and not pd.isna(row[field]):
                    golden_record[field] = row[field]
                    replaced_ids[field] = row["client_id"]
            if len(replaced_ids) == len(nan_fields):
                break
        golden_record["mapping_fields"] = json.dumps(replaced_ids)
        result_grouped.append(golden_record)
    return pd.DataFrame(result_grouped), result_unique_records