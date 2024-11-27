import argparse
import time

import pandas as pd
import logging
from modules.similar import find_similar_data

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

    logging.info(f"results = {results.head(5)}")


if __name__ == "__main__":
    logging.info("Starting working...")
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        logging.error(e)
    logging.info("End working...")
