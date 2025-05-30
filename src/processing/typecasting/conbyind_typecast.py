from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, StringType, IntegerType, LongType
from src.utils.logger import setup_logger

logger = setup_logger("conbyind_typecast", "logs/processing.log")


def typecast_ConByInd_df(df):
    try:
        df = (
            df.withColumn("TRANSACTION_AMT", col("TRANSACTION_AMT").cast(DoubleType()))
            .withColumn("ZIP_CODE", col("ZIP_CODE").cast(StringType()))
            .withColumn("TRANSACTION_DT", to_date(col("TRANSACTION_DT"), "MMddyyyy"))
            .withColumn("FILE_NUM", col("FILE_NUM").cast(IntegerType()))
            .withColumn("SUB_ID", col("SUB_ID").cast(LongType()))
        )
        logger.info("ConByInd DataFrame typecasting completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during ConByInd DataFrame typecasting: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
