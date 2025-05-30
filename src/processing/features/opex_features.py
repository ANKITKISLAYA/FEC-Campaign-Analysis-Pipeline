from pyspark.sql.functions import upper, year, month, col
from src.utils.logger import setup_logger

logger = setup_logger("opex_features", "logs/processing.log")


def features_OpEx_df(df):
    try:
        # Extract features YEAR and MONTH and normalise STATE , CATEGORY,
        # CATEGORY_DESC, ENTITY_TP in upper case
        df = (
            df.withColumn("YEAR", year(col("TRANSACTION_DT")))
            .withColumn("MONTH", month(col("TRANSACTION_DT")))
            .withColumn("STATE", upper(col("STATE")))
            .withColumn("CATEGORY", upper(col("CATEGORY")))
            .withColumn("CATEGORY_DESC", upper(col("CATEGORY_DESC")))
            .withColumn("ENTITY_TP", upper(col("ENTITY_TP")))
        )
        logger.info("OpEx features added successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during OpEx features extraction: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
