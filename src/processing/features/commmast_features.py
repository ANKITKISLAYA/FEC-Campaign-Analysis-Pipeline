from pyspark.sql.functions import col, when, upper
from src.utils.logger import setup_logger

logger = setup_logger("commmast_features", "logs/processing.log")


def features_CommMast_df(df):
    try:

        # Flags for committee type and designation
        # Normalize party names
        # Flag candidate-linked committees
        df = (
            df.withColumn("CMTE_TP", upper(col("CMTE_TP")))
            .withColumn("CMTE_PTY_AFFILIATION", upper(col("CMTE_PTY_AFFILIATION")))
            .withColumn(
                "IS_CAND_LINKED", when(col("CAND_ID").isNotNull(), 1).otherwise(0)
            )
            .withColumn("IS_AUTH_CMTE", when(col("CMTE_DSGN") == "A", 1).otherwise(0))
            .withColumn(
                "IS_PAC", when(col("CMTE_TP").isin("Q", "N", "O", "U"), 1).otherwise(0)
            )
            .withColumn("IS_PARTY_CMTE", when(col("CMTE_TP") == "Y", 1).otherwise(0))
        )
        logger.info("CommMast features added successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during CommMast features extraction: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
