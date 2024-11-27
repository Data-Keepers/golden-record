from splink import block_on, SettingsCreator
import splink.comparison_library as cl
from splink import Linker, DuckDBAPI
import pandas as pd


def find_similar_data(df: pd.DataFrame) -> pd.DataFrame:
    df['client_first_name'] = df['client_first_name'].str.lower()
    df['client_last_name'] = df['client_last_name'].str.lower()
    df['client_middle_name'] = df['client_middle_name'].str.lower()
    df['client_fio_full'] = df['client_fio_full'].str.lower()
    settings = SettingsCreator(
        unique_id_column_name="client_id",
        link_type="dedupe_only",
        blocking_rules_to_generate_predictions=[
            block_on("client_first_name", "client_middle_name", "client_last_name", "client_bday"),
            block_on("client_fio_full", "client_bday"),
            block_on("client_inn"),
            block_on("client_snils"),
            block_on("contact_email")

        ],
        comparisons=[
            cl.ForenameSurnameComparison(
                "client_first_name",
                "client_last_name",
                forename_surname_concat_col_name=None,
            ),
            cl.ForenameSurnameComparison(
                "client_first_name",
                "client_last_name",
                forename_surname_concat_col_name=None,
            ),
            cl.DateOfBirthComparison(
                "client_bday",
                input_is_string=True,
            ),
            # cl.JaroWinklerAtThresholds("client_bplace", 0.9).configure(
            #     term_frequency_adjustments=True
            # ),
        ],
    )

    deterministic_rules = [
        "l.client_first_name = r.client_first_name",
    ]

    linker = Linker(df, settings, db_api=DuckDBAPI(), set_up_basic_logging=False)

    linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.6)

    linker.training.estimate_u_using_random_sampling(max_pairs=2e6)
    df_predict = linker.inference.predict(threshold_match_probability=0.9)
    results = linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, threshold_match_probability=0.9)
    return results.as_pandas_dataframe()
