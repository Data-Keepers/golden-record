from splink import block_on, SettingsCreator
import splink.comparison_library as cl
from splink import Linker, DuckDBAPI
from transliterate import translit
import pandas as pd


def transliterate_to_cyrillic(text):
    if not isinstance(text, str) or text.strip() == "":
        return text
    try:
        return translit(text, 'ru')
    except:
        return text


def find_similar_data(df: pd.DataFrame) -> pd.DataFrame:
    df['client_first_name'] = df['client_first_name'].str.lower().apply(transliterate_to_cyrillic)
    df['client_last_name'] = df['client_last_name'].str.lower().apply(transliterate_to_cyrillic)
    df['client_middle_name'] = df['client_middle_name'].str.lower().apply(transliterate_to_cyrillic)
    df['client_fio_full'] = df['client_fio_full'].str.lower().apply(transliterate_to_cyrillic)
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
            cl.ExactMatch("client_inn").configure(
                m_probabilities=[0.99, 0.01],  # 100% совпадение, вероятность для несовпадения = 0
                u_probabilities=[0.01, 0.99]  # 100% несовпадение
            ),
            cl.ExactMatch("client_snils").configure(
                m_probabilities=[0.99, 0.01],  # 100% совпадение, вероятность для несовпадения = 0
                u_probabilities=[0.01, 0.99]  # 100% несовпадение
            ),
            cl.ExactMatch("contact_email").configure(
                m_probabilities=[0.5, 0.5],  # 50% совпадение, вероятность для несовпадения = 0
                u_probabilities=[0.5, 0.5]  # 50% несовпадение
            ),
            cl.ExactMatch("contact_phone").configure(
                m_probabilities=[0.5, 0.5],  # 50% совпадение, вероятность для несовпадения = 0
                u_probabilities=[0.5, 0.5]  # 50% несовпадение
            ),
            cl.ForenameSurnameComparison(
                forename_col_name="client_first_name",
                surname_col_name="client_last_name",
            ).configure(
                m_probabilities=[0.6, 0.2, 0.05, 0.6, 0.2, 0.05, 0.1],  # 7 значений
                u_probabilities=[0.02, 0.05, 0.1, 0.02, 0.05, 0.1, 0.66]  # 7
            ),
            cl.ForenameSurnameComparison(
                forename_col_name="client_first_name",
                surname_col_name="client_middle_name",
            ).configure(
                m_probabilities=[0.6, 0.2, 0.05, 0.6, 0.2, 0.05, 0.1],  # 7 значений
                u_probabilities=[0.02, 0.05, 0.1, 0.02, 0.05, 0.1, 0.66]  # 7
            ),
            cl.ForenameSurnameComparison(
                forename_col_name="client_middle_name",
                surname_col_name="client_last_name",
            ).configure(
                m_probabilities=[0.6, 0.2, 0.05, 0.6, 0.2, 0.05, 0.1],  # 7 значений
                u_probabilities=[0.02, 0.05, 0.1, 0.02, 0.05, 0.1, 0.66]  # 7
            ),
            cl.DateOfBirthComparison(
                "client_bday",
                input_is_string=True,
            ).configure(
                m_probabilities=[0.6, 0.1, 0.1, 0.05, 0.05, 0.1],  # 6 уровней для совпадений
                u_probabilities=[0.02, 0.02, 0.02, 0.4, 0.4, 0.12]  # 6 уровней для несовпадений
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
    return results.as_pandas_dataframe()
