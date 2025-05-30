from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, IntegerType
from src.utils.logger import setup_logger

logger = setup_logger("tranonecomtoano_typecast", "logs/processing.log")


def typecast_TranOneComToAno_df(df):
    try:
        df = df.withColumn("TRANSACTION_AMT", col("TRANSACTION_AMT").cast(DoubleType()))
        df = df.withColumn("FILE_NUM", col("FILE_NUM").cast(IntegerType()))

        # Convert date format (assuming MMDDYYYY or similar)
        df = df.withColumn("TRANSACTION_DT", to_date(col("TRANSACTION_DT"), "MMddyyyy"))
        logger.info("TranOneComToAno DataFrame typecasting completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during TranOneComToAno DataFrame typecasting: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
