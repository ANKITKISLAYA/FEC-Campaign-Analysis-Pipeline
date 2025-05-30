from pyspark.sql.functions import col
from src.utils.logger import setup_logger

logger = setup_logger("conbyind_clean", "logs/processing.log")


# Cleaning Contributions by individuals df
def clean_ConByInd_df(df):
    try:
        # Filter out rows missing truly critical information
        df = df.filter(col("TRANSACTION_AMT").isNotNull())
        df = df.filter(col("TRANSACTION_DT").isNotNull())
        df = df.filter(col("SUB_ID").isNotNull())

        # Keep only individual contributions
        df = df.filter(col("OTHER_ID").isNull())

        # Drop legacy columns
        cols_to_drop = ["OTHER_ID", "MEMO_CD", "MEMO_TEXT"]
        df = df.drop(*cols_to_drop)

        # Replace nulls using fillna
        df = df.fillna(
            {
                "TRANSACTION_PGI": "O",
                "ENTITY_TP": "IND",  # null likely indicates an unrecorded individual contributor
                "NAME": "UNKNOWN",
                "CITY": "UNKNOWN",
                "STATE": "NA",
                "ZIP_CODE": "00000",
                "EMPLOYER": "UNKNOWN",
                "OCCUPATION": "UNKNOWN",
                "TRAN_ID": "MISSING",
                "FILE_NUM": 0,
            }
        )
        logger.info("ConByInd data cleaning completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during Contributions by Individuals data cleaning: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
