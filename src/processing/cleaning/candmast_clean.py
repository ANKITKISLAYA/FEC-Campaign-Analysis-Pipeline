from src.utils.logger import setup_logger

logger = setup_logger("candmast_clean", "logs/processing.log")


def clean_CandMast_df(df):
    try:
        # Drop redundant or rarely useful columns
        df = df.drop("CAND_ST1", "CAND_ST2", "CAND_PCC")

        # Handle nulls / empty values
        df = df.fillna(
            {
                "CAND_PTY_AFFILIATION": "UNK",  # Unknown party
                "CAND_OFFICE_DISTRICT": "-1",  # -1 means non-district-based
                "CAND_ICI": "U",  # U = Unknown status
                "CAND_CITY": "UNKNOWN",
                "CAND_ST": "NA",
                "CAND_ZIP": "00000",
            }
        )
        logger.info("CandMast data cleaning completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during CandMast data cleaning: {e}", exc_info=True
        )
        raise


if __name__ == "__main__":
    pass
