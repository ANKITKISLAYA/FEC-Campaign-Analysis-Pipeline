from src.utils.logger import setup_logger

logger = setup_logger("candmast_clean", "logs/processing.log")


# Cleaning any transaction from one committee to another df
def clean_TranOneComToAno_df(df):
    try:

        # Drop legacy columns that have too many nulls
        cols_to_drop = [
            "EMPLOYER",
            "OCCUPATION",
            "OTHER_ID",
            "MEMO_CD",
            "MEMO_TEXT",
            "SUB_ID",
        ]
        df = df.drop(*cols_to_drop)

        # Handle nulls with default values or flags
        df = df.fillna(
            {
                "TRANSACTION_PGI": "U0000",  # Unknown election year
                "ENTITY_TP": "UNK",  # Unknown entity
                "NAME": "Unknown Name",
                "CITY": "Unknown City",
                "STATE": "NA",
                "ZIP_CODE": "00000",
                "TRANSACTION_DT": "01011900",  # Dummy old date
                "TRAN_ID": "UNKNOWN",
            }
        )
        logger.info("TranOneComToAno data cleaning completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during Transactions from One Committee to Another data cleaning: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
