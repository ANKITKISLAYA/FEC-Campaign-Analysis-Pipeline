from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType
from src.utils.logger import setup_logger

logger = setup_logger("allcand_typecast", "logs/processing.log")


def typecast_AllCand_df(df):
    try:

        # List of columns to cast as DoubleType
        double_cols = [
            "TTL_RECEIPTS",
            "TRANS_FROM_AUTH",
            "TTL_DISB",
            "TRANS_TO_AUTH",
            "COH_BOP",
            "COH_COP",
            "CAND_CONTRIB",
            "CAND_LOANS",
            "OTHER_LOANS",
            "CAND_LOAN_REPAY",
            "OTHER_LOAN_REPAY",
            "DEBTS_OWED_BY",
            "TTL_INDIV_CONTRIB",
            "OTHER_POL_CMTE_CONTRIB",
            "POL_PTY_CONTRIB",
            "INDIV_REFUNDS",
            "CMTE_REFUNDS",
        ]

        # Cast numeric columns to Double
        for col_name in double_cols:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

        # Cast CVG_END_DT to DateType (assuming MM/dd/yyyy format)
        df = df.withColumn("CVG_END_DT", to_date(col("CVG_END_DT"), "MM/dd/yyyy"))
        logger.info("AllCand DataFrame typecasting completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during AllCand DataFrame typecasting: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
