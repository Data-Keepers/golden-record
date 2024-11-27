import argparse
import time

import pandas as pd
import logging
from modules.similar import find_similar_data
from modules.union_records import union_records_by_cluster_id

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Creating a golden records!")
    parser.add_argument('--file', type=str, help='file', required=True)
    args = parser.parse_args()
    return args


def main():
    args = parse_arguments()

    logging.info(f"Start read file {args.file}")
    start_time = time.time()
    df = pd.read_csv(args.file, sep=',', quotechar='"', dtype=str)
    df['unique_id'] = df.index
    logging.info(f"Finished read file {args.file} - elapsed time: {round(time.time() - start_time, 3)} seconds")

    logging.info("Start find similar data")
    start_time = time.time()
    results = find_similar_data(df)
    logging.info(f"Finished find similar data - elapsed time: {round(time.time() - start_time, 3)} seconds")
    # results.to_csv("results.csv", index=False, encoding="utf-8")
    # results = pd.read_csv("results.csv", encoding="utf-8")

    logging.info("Start union calculation golden record")
    start_time = time.time()
    result_grouped, result_unique_records = union_records_by_cluster_id(results)
    logging.info(f"Finished calculation golden record length grouped - {len(result_grouped)}, "
                 f"length unique {len(result_unique_records)} - elapsed time: {round(time.time() - start_time, 3)} seconds")

    logging.info("Save golden records to assets/grouped_records.csv, assets/unique_records.csv, assets/golden_records.csv")
    start_time = time.time()
    result_grouped.to_csv("assets/grouped_records.csv", index=False)
    result_unique_records.to_csv("assets/unique_records.csv", index=False)
    pd.concat([result_grouped, result_unique_records], ignore_index=True).to_csv("assets/golden_records.csv", index=False)
    logging.info(f"Finished save golden records - elapsed time: {round(time.time() - start_time, 3)} seconds")


if __name__ == "__main__":
    start_time = time.time()
    logging.info("Starting working...")
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        logging.error(e)
    logging.info(f"End working - elapsed time {round(start_time - time.time(), 3)} sec")
