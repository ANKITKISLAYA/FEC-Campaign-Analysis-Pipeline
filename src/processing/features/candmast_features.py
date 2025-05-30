from pyspark.sql.functions import col, when, upper
from src.utils.logger import setup_logger

logger = setup_logger("candmast_features", "logs/processing.log")


def features_CandMast_df(df):
    try:
        # Create flags and normalized versions
        # Map CAND_ICI (Incumbent/Challenger/Open Seat)
        df = (
            df.withColumn("CAND_OFFICE", upper(col("CAND_OFFICE")))
            .withColumn("CAND_PTY_AFFILIATION", upper(col("CAND_PTY_AFFILIATION")))
            .withColumn("IS_INCUMBENT", when(col("CAND_ICI") == "I", 1).otherwise(0))
            .withColumn("IS_CHALLENGER", when(col("CAND_ICI") == "C", 1).otherwise(0))
            .withColumn("IS_OPEN_SEAT", when(col("CAND_ICI") == "O", 1).otherwise(0))
        )
        logger.info("CandMast features added successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during CandMast features extraction: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
