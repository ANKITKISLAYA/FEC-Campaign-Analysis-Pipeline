from src.utils.logger import setup_logger
from pyspark.sql.functions import col, round

logger = setup_logger("explore", "logs/ingestion.log")


def check_data_shape(df, column_count, row_count):
    """
    Checks if the shape of the DataFrame matches the expected shape.

    Parameters:
        df (DataFrame): The Spark DataFrame to check.
        column_count (int): Expected number of columns in the DataFrame.
        row_count (int): Expected number of rows in the DataFrame.
    """

    if df.count() == row_count and len(df.columns) == column_count:
        logger.info(f"Shape of {df} matches the documentation")
    else:
        logger.info(f"Shape of {df} doesn't matches the documentation")


def generate_summary_stats(numerical_columns, df):
    """
    Returns dataframe containing statistical summary of df.

    Parameters:
        numerical_columns (list): The Spark DataFrame.
        df (DataFrame): The Spark DataFrame.
    """

    summary_df = df.select(numerical_columns).describe()

    # Round numerical values to 2 decimal places
    rounded_summary_df = summary_df.select(
        "summary",
        *[round(col(c), 2).alias(c) for c in summary_df.columns if c != "summary"],
    )
    return rounded_summary_df


if __name__ == "__main__":
    pass
