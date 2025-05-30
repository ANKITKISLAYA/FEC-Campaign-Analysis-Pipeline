from pyspark.sql.functions import col
from src.utils.logger import setup_logger

logger = setup_logger("conbyind_clean", "logs/processing.log")


def clean_ConFromComToCanIndExpen_df(df):
    try:
        # Filter Columns where OTHER_ID is notNull since Null
        # indicates contributions from individuals
        df = df.filter(col("OTHER_ID").isNotNull())

        # Filter Columns where CAND_ID is notNUll
        df = df.filter(col("CAND_ID").isNotNull())

        # Replace nulls using fillna
        df = df.fillna(
            {
                "TRANSACTION_PGI": "UKN",
                "ENTITY_TP": "COM",  # the contributor is typically a committee here in com to cand contr
                "NAME": "UNKNOWN",
                "CITY": "UNKNOWN",
                "STATE": "NA",
                "ZIP_CODE": "00000",
                "TRANSACTION_DT": "12319999",
                "TRAN_ID": "MISSING",
            }
        )

        # Drop rarely used or high-null columns
        cols_to_drop = ["EMPLOYER", "OCCUPATION", "OTHER_ID", "MEMO_CD", "MEMO_TEXT"]
        df = df.drop(*cols_to_drop)
        logger.info("ConFromComToCanIndExpen data cleaning completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during Contributions from Committees to Candidates data cleaning: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
