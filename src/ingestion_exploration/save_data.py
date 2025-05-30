from src.utils.logger import setup_logger

logger = setup_logger("save_data", "logs/ingestion.log")


def save_to_parquet(df, output_path, df_name):
    """
    Writes Spark DataFrame to a Parquet file at the given GCS path.

    Parameters:
        df (DataFrame): The Spark DataFrame.
        output_path (str): GCS path to save the Parquet file.
        df_name (str): Name to use for the output directory.

    Returns:
        None
    """
    try:
        logger.info(f"Saving DataFrame to Parquet at {output_path}/{df_name}.")

        # Construct final output path
        full_output_path = f"{output_path}/{df_name}"

        # Write to Parquet
        df.write.mode("overwrite").parquet(full_output_path)
    except Exception as e:
        logger.error(f"Error saving DataFrame to Parquet: {e}", exc_info=True)
        raise
    else:
        logger.info(
            f"DataFrame successfully saved to {full_output_path} in Parquet format."
        )


if __name__ == "__main__":
    pass
