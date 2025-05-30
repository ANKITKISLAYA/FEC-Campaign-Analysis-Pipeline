from pyspark.sql.functions import col, when, lit
from src.utils.logger import setup_logger

logger = setup_logger("pacsum_clean", "logs/processing.log")


def clean_PacSum_df(df):
    try:
        # Drop legacy columns
        legacy_cols = [
            "CMTE_DSGN",
            "CAND_CONTRIB",
            "CAND_LOANS",
            "TTL_LOANS_RECEIVED",
            "CAND_LOAN_REPAY",
            "OTHER_POL_CMTE_REFUNDS",
            "INDV_REFUNDS",
            "NONFED_TRANS_RECEIVED",
            "NONFED_SHARE_EXP",
        ]
        df = df.drop(*legacy_cols)

        # Fill nulls in numeric columns with 0.0
        numeric_cols = [
            "TTL_RECEIPTS",
            "TRANS_FROM_AFF",
            "INDV_CONTRIB",
            "OTHER_POL_CMTE_CONTRIB",
            "TTL_DISB",
            "TRANF_TO_AFF",
            "LOAN_REPAY",
            "COH_BOP",
            "COH_COP",
            "DEBTS_OWED_BY",
            "CONTRIB_TO_OTHER_CMTE",
            "IND_EXP",
            "PTY_COORD_EXP",
        ]
        for col_name in numeric_cols:
            df = df.withColumn(
                col_name,
                when(col(col_name).isNull(), lit("0.0")).otherwise(col(col_name)),
            )

        # Handle nulls in date
        df = df.withColumn(
            "CVG_END_DT",
            when(col("CVG_END_DT").isNull(), lit("12/31/9999")).otherwise(
                col("CVG_END_DT")
            ),
        )
        logger.info("PacSum data cleaning completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during PacSum data cleaning: {e}", exc_info=True
        )
        raise


if __name__ == "__main__":
    pass
