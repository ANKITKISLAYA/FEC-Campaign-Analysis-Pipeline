from src.utils.logger import setup_logger

logger = setup_logger("conbyind_clean", "logs/processing.log")


def clean_HouSenCurCam_df(df):
    try:
        # Fill NUll value for col CAND_ICI as U (Unkown)
        df = df.fillna({"CAND_ICI": "U"})

        # Fill nulls in CVG_END_DT with default date
        df = df.fillna({"CVG_END_DT": "12/31/9999"})

        # Drop rarely used or high-null columns
        cols_to_drop = [
            "SPEC_ELECTION",
            "PRIM_ELECTION",
            "RUN_ELECTION",
            "GEN_ELECTION",
            "GEN_ELECTION_PRECENT",
        ]
        df = df.drop(*cols_to_drop)
        logger.info(
            "House, Senate, and Current Campaign data cleaning completed successfully."
        )
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during House, Senate, and Current Campaign data cleaning: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
