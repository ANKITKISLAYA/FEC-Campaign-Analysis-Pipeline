from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType
from src.utils.logger import setup_logger

logger = setup_logger("candmast_typecast", "logs/processing.log")

def typecast_CandMast_df(df):
    try:

        df = df.withColumn("CAND_ELECTION_YR", col("CAND_ELECTION_YR").cast(IntegerType())) \
            .withColumn("CAND_OFFICE_DISTRICT", col("CAND_OFFICE_DISTRICT").cast(IntegerType())) \
            .withColumn("CAND_ZIP", col("CAND_ZIP").cast(StringType()))
        logger.info("CandMast DataFrame typecasting completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during CandMast DataFrame typecasting: {e}",
            exc_info=True,
        )
        raise

if __name__ == "__main__":
    pass