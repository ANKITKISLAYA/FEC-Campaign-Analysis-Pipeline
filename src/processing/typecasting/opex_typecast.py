from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, LongType, IntegerType
from src.utils.logger import setup_logger

logger = setup_logger("opex_typecast", "logs/processing.log")


def typecast_OpEx_df(df):
    try:
        df = (
            df.withColumn("TRANSACTION_AMT", col("TRANSACTION_AMT").cast(DoubleType()))
            .withColumn("SUB_ID", col("SUB_ID").cast(LongType()))
            .withColumn("FILE_NUM", col("FILE_NUM").cast(IntegerType()))
            .withColumn("RPT_YR", col("RPT_YR").cast(IntegerType()))
            .withColumn("TRANSACTION_DT", to_date(col("TRANSACTION_DT"), "MM/dd/yyyy"))
            .withColumn("ZIP_CODE", col("ZIP_CODE").cast("string"))
        )
        logger.info("OpEx DataFrame typecasting completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during OpEx DataFrame typecasting: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
