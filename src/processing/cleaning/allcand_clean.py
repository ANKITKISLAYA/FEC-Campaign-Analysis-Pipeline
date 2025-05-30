from pyspark.sql.functions import col, when
from src.utils.logger import setup_logger

logger = setup_logger("allcand_clean", "logs/processing.log")


def clean_AllCand_df(df):
    try:
        # Fill nulls for CAND_ICI with 'U' (Unknown)
        df = df.withColumn(
            "CAND_ICI", when(col("CAND_ICI").isNull(), "U").otherwise(col("CAND_ICI"))
        )

        # Fill nulls for CAND_PTY_AFFILIATION with 'UNK'
        df = df.withColumn(
            "CAND_PTY_AFFILIATION",
            when(col("CAND_PTY_AFFILIATION").isNull(), "UNK").otherwise(
                col("CAND_PTY_AFFILIATION")
            ),
        )

        # Drop legacy or mostly-null columns
        df = df.drop(
            "SPEC_ELECTION",
            "PRIM_ELECTION",
            "RUN_ELECTION",
            "GEN_ELECTION",
            "GEN_ELECTION_PRECENT",
        )
        logger.info("AllCand data cleaning completed successfully.")
        return df

    except Exception as e:
        logger.error(f"An error occurred during data cleaning: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    pass
