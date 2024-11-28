import argparse
import time

import pandas as pd
import logging

from modules.data_cleaning import clean_data
from modules.data_preparing import prepare_data
from modules.similar import find_similar_data
from modules.union_records import union_records_by_cluster_id

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Creating a golden records!")
    parser.add_argument('-f', type=str, help='file name', required=True)
    parser.add_argument('-o', type=str, help='output file name', required=True)
    args = parser.parse_args()
    return args


def main():
    args = parse_arguments()
    logging.info(f"Starting to read file {args.f}...")
    start_time = time.time()
    df = pd.read_csv(args.f, sep=',', quotechar='"', dtype=str)
    logging.info(f"Finished read file {args.f} - elapsed time: {round(time.time() - start_time, 3)} seconds")
    logging.info(f"Total row count: {len(df)}")

    df = prepare_data(df)

    results = find_similar_data(df)

    result_grouped = union_records_by_cluster_id(results)

    result_cleaned = clean_data(result_grouped)

    logging.info(f"Save golden records to file {args.o}, rows count: {len(result_cleaned)}")

    start_time = time.time()
    result_cleaned.to_csv(args.o, index=False)
    logging.info(f"Saving completed, elapsed time: {round(time.time() - start_time, 3)} seconds")


if __name__ == "__main__":
    start_time = time.time()
    logging.info("Start working...")
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        logging.error(e)
    logging.info(f"Total elapsed time {round(time.time() - start_time, 3)} sec")
