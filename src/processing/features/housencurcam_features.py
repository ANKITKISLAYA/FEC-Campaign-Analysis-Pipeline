from pyspark.sql.functions import col
from src.utils.logger import setup_logger

logger = setup_logger("housencurcam_features", "logs/processing.log")

def features_HouSenCurCam_df(df):
    try:

        # Create Feature Total loans, repay, contributions and net reciepts
        df = df.withColumn("TOTAL_LOANS", col("CAND_LOANS") + col("OTHER_LOANS")) \
            .withColumn("TOTAL_REPAY", col("CAND_LOAN_REPAY") + col("OTHER_LOAN_REPAY")) \
            .withColumn("NET_RECEIPTS", col("TTL_RECEIPTS") - col("TTL_DISB")) \
            .withColumn("TOTAL_CONTRIB", col("TTL_INDIV_CONTRIB") + col("OTHER_POL_CMTE_CONTRIB") + col("POL_PTY_CONTRIB"))
        logger.info("HouSenCurCam features added successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during HouSenCurCam features extraction: {e}",
            exc_info=True,
        )
        raise

if __name__ == "__main__":
    pass