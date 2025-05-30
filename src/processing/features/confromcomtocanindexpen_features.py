from pyspark.sql.functions import upper, year, month, weekofyear, col
from src.utils.logger import setup_logger

logger = setup_logger("confromcomtotoanind_features", "logs/processing.log")

def features_ConFromComToCanIndExpen_df(df):
    try:

        # Extract year, month, week and normalize State
        df = df.withColumn("YEAR", year(col("TRANSACTION_DT"))) \
            .withColumn("MONTH", month(col("TRANSACTION_DT"))) \
            .withColumn("WEEK", weekofyear(col("TRANSACTION_DT"))) \
            .withColumn("STATE", upper(col("STATE"))) \
            .withColumn("ENTITY_TP", upper(col("ENTITY_TP"))) \
            .withColumn("TRANSACTION_TP", upper(col("TRANSACTION_TP")))
        logger.info("ConFromComToCanIndExpen features added successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during ConFromComToCanIndExpen features extraction: {e}",
            exc_info=True,
        )
        raise

if __name__ == "__main__":
    pass