from src.utils.logger import setup_logger

logger = setup_logger("candmast_clean", "logs/processing.log")


def clean_CommMast_df(df):
    try:
        # Drop mostly null or redundant address fields
        df = df.drop("CMTE_ST1", "CMTE_ST2")

        # Fill nulls with appropriate default values
        df = df.fillna(
            {
                "CMTE_NM": "UNKNOWN_COMMITTEE",
                "TRES_NM": "UNKNOWN_TREASURER",
                "CMTE_CITY": "UNKNOWN_CITY",
                "CMTE_ST": "NA",
                "CMTE_ZIP": "00000",
                "CMTE_DSGN": "U",  # Unknown designation
                "CMTE_TP": "U",  # Unknown type
                "CMTE_PTY_AFFILIATION": "OTH",  # Other
                "ORG_TP": "N/A",
                "CONNECTED_ORG_NM": "NONE",
                "CAND_ID": "UNLINKED",
            }
        )
        logger.info("CommMast data cleaning completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during CommMast data cleaning: {e}", exc_info=True
        )
        raise


if __name__ == "__main__":
    clean_CommMast_df()
