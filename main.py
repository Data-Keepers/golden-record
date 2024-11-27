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

    logging.info("Starting to search for similar rows...")
    start_time = time.time()
    results = find_similar_data(df)
    logging.info(f"Search for similar rows completed - elapsed time: {round(time.time() - start_time, 3)} seconds")

    logging.info("Starting to merge similar rows...")
    start_time = time.time()
    result_grouped, result_unique_records = union_records_by_cluster_id(results)
    logging.info(f"""Row merging completed.
                 Duplicated row count: {len(result_grouped)}
                 Unique rows count:{len(result_unique_records)}
                 Elapsed time: {round(time.time() - start_time, 3)} seconds""")

    logging.info(f"Save golden records to file {args.o}")

    start_time = time.time()
    pd.concat([result_grouped, result_unique_records], ignore_index=True).to_csv(args.o, index=False)
    logging.info(f"Saving completed - elapsed time: {round(time.time() - start_time, 3)} seconds")


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
