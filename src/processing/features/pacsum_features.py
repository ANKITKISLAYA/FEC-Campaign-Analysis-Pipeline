from pyspark.sql.functions import col, when, year
from src.utils.logger import setup_logger

logger = setup_logger("pacsum_features", "logs/processing.log")


def features_PacSum_df(df):
    try:

        # calculating net cash flow, Contribution ratios, Running balances, Year extraction
        df = df.withColumn("NET_CASH_FLOW", col("TTL_RECEIPTS") - col("TTL_DISB"))
        df = df.withColumn(
            "INDV_CONTRIB_RATIO",
            when(
                col("TTL_RECEIPTS") != 0, col("INDV_CONTRIB") / col("TTL_RECEIPTS")
            ).otherwise(0),
        )
        df = df.withColumn("YEAR", year(col("CVG_END_DT")))
        logger.info("PacSum features added successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during PacSum features extraction: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    features_PacSum_df()
