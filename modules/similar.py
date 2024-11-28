import logging
import time
from splink import block_on, SettingsCreator
import splink.comparison_library as cl
from splink import Linker, DuckDBAPI
import pandas as pd


def find_similar_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting to search for similar rows...")
    start_time = time.time()

    # Распараллеливаем транслитерацию
    logging.info("Starting parallel transliteration...")

    logging.info(f"Transliteration completed, elapsed time: {round(time.time() - start_time, 3)} seconds")

    # Настройки для Splink
    settings = SettingsCreator(
        unique_id_column_name="client_id",
        link_type="dedupe_only",
        blocking_rules_to_generate_predictions=[
            block_on("client_first_name", "client_middle_name", "client_last_name", "client_bday"),
            block_on("client_fio_full", "client_bday"),
            block_on("client_inn"),
            block_on("client_snils"),
            block_on("contact_email"),
            block_on("contact_phone")
        ],
        comparisons=[
            cl.JaroWinklerAtThresholds("client_inn"),
            cl.JaroWinklerAtThresholds("client_snils"),
            cl.EmailComparison("contact_email"),
            cl.JaroWinklerAtThresholds("contact_phone"),
            cl.NameComparison("client_first_name"),
            cl.NameComparison("client_middle_name"),
            cl.NameComparison("client_last_name"),
            cl.NameComparison("client_fio_full"),
            cl.DateOfBirthComparison(
                "client_bday",
                input_is_string=True,
            )
        ],
    )

    deterministic_rules = [
        "l.client_inn = r.client_inn"
    ]

    linker = Linker(df, settings, db_api=DuckDBAPI(), set_up_basic_logging=False)
    linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.6)

    linker.training.estimate_u_using_random_sampling(max_pairs=2e6)
    df_predict = linker.inference.predict(threshold_match_probability=0.9)
    results = linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, threshold_match_probability=0.9)

    logging.info(f"Search for similar rows completed, elapsed time: {round(time.time() - start_time, 3)} seconds")
    return results.as_pandas_dataframe()
