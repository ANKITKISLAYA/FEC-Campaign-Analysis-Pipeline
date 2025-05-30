from pyspark.sql.functions import col, when, trim
from src.utils.logger import setup_logger

logger = setup_logger("base_clean", "logs/processing.log")


def base_clean(df):
    try:
        # Trim all string columns
        for c in df.columns:
            df = df.withColumn(
                c,
                when(
                    col(c).isNotNull() & (df[c].cast("string").isNotNull()),
                    trim(col(c)),
                ).otherwise(col(c)),
            )

        # Replace "", "NA", "N/A" with None
        for c in df.columns:
            df = df.withColumn(
                c, when((col(c).isin("", "NA", "N/A")), None).otherwise(col(c))
            )
        logger.info("Base data cleaning completed successfully.")
        return df
    except Exception as e:
        logger.error(f"An error occurred during base data cleaning: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    pass
