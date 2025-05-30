from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from src.utils.logger import setup_logger

logger = setup_logger("cancomlink_typecast", "logs/processing.log")


def typecast_CanComLink_df(df):
    try:
        df = (
            df.withColumn(
                "CAND_ELECTION_YR", col("CAND_ELECTION_YR").cast(IntegerType())
            )
            .withColumn("FEC_ELECTION_YR", col("FEC_ELECTION_YR").cast(IntegerType()))
            .withColumn("LINKAGE_ID", col("LINKAGE_ID").cast(IntegerType()))
        )
        logger.info("CanComLink DataFrame typecasting completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during CanComLink DataFrame typecasting: {e}",
            exc_info=True,
        )


if __name__ == "__main__":
    pass
