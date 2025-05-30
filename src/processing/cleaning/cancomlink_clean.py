from pyspark.sql.functions import col, trim
from src.utils.logger import setup_logger

logger = setup_logger("cancomlink_clean", "logs/processing.log")


def clean_CanComLink_df(df):
    try:
        # Trim all string columns
        for column in df.columns:
            df = df.withColumn(column, trim(col(column)))

        # Drop duplicates
        df = df.dropDuplicates()
        logger.info("CanComLink data cleaning completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during CanComLink data cleaning: {e}", exc_info=True
        )
        raise


if __name__ == "__main__":
    pass
