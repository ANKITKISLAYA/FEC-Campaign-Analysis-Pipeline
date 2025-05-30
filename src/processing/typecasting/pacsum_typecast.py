from pyspark.sql.functions import to_date
from pyspark.sql.types import DoubleType
from src.utils.logger import setup_logger

logger = setup_logger("pacsum_typecast", "logs/processing.log")


def typecast_PacSum_df(df):
    try:
        # numeric cols
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
            df = df.withColumn(col_name, df[col_name].cast(DoubleType()))

        df = df.withColumn("CVG_END_DT", to_date(df["CVG_END_DT"], "MM/dd/yyyy"))
        logger.info("PacSum DataFrame typecasting completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during PacSum DataFrame typecasting: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
