from src.utils.logger import setup_logger

logger = setup_logger("candmast_clean", "logs/processing.log")


def clean_OpEx_df(df):
    try:
        # Drop irrelevant or mostly-null columns
        cols_to_drop = ["MEMO_CD", "MEMO_TEXT", "BACK_REF_TRAN_ID", "extra_column"]
        df = df.drop(*cols_to_drop)

        # Handle Null values
        fill_values = {
            "NAME": "UNKNOWN",
            "CITY": "UNKNOWN",
            "STATE": "UNK",
            "ZIP_CODE": "00000",
            "TRANSACTION_DT": "12/31/9999",
            "TRANSACTION_PGI": "N/A",
            "PURPOSE": "UNSPECIFIED",
            "CATEGORY": "MISC",
            "CATEGORY_DESC": "MISCELLANEOUS",
            "ENTITY_TP": "UNKNOWN",
        }
        df = df.fillna(fill_values)
        logger.info("OpEx data cleaning completed successfully.")
        return df
    except Exception as e:
        logger.error(f"An error occurred during OpEx data cleaning: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    pass
