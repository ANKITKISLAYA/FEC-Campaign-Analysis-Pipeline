from pyspark.sql.functions import to_date
from pyspark.sql.types import DoubleType
from src.utils.logger import setup_logger

logger = setup_logger("housencurcam_typecast", "logs/processing.log")


def typecast_HouSenCurCam_df(df):
    try:
        # Financial fields
        money_cols = [
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
        for col_name in money_cols:
            df = df.withColumn(col_name, df[col_name].cast(DoubleType()))

        df = df.withColumn("CVG_END_DT", to_date(df["CVG_END_DT"], "MM/dd/yyyy"))
        logger.info("HouSenCurCam DataFrame typecasting completed successfully.")

        return df
    except Exception as e:
        logger.error(
            f"An error occurred during HouSenCurCam DataFrame typecasting: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
