from pyspark.sql.functions import col, year, month, weekofyear, upper, regexp_replace
from src.utils.logger import setup_logger

logger = setup_logger("conbyind_features", "logs/processing.log")


def features_ConByInd_df(df):
    try:

        # Extract year, month, week and normalize State
        df = (
            df.withColumn("YEAR", year(col("TRANSACTION_DT")))
            .withColumn("MONTH", month(col("TRANSACTION_DT")))
            .withColumn("WEEK", weekofyear(col("TRANSACTION_DT")))
            .withColumn("STATE", upper(col("STATE")))
        )  # Normalize state names

        # Normalize donor names by removing middle name
        df = df.withColumn("NAME", regexp_replace("NAME", r"\s+[A-Z]\.$", ""))
        logger.info("ConByInd features added successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during ConByInd features extraction: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
