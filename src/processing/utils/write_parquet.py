from src.utils.logger import setup_logger

logger = setup_logger("write_parquet", "logs/processing.log")


def write_df_to_parquet(
    df,
    file_name,
    base_path="gs://dataproc-staging-us-central1-40371648517-ndvgfbwp/notebooks/jupyter/FEC-Campaign-Analysis/FEC-Data/silver",
):
    """
    Writes a Spark DataFrame to the specified GCS path in Parquet format.

    Args:
        df (DataFrame): Spark DataFrame to write.
        file_name (str): Folder name (like table name) for Parquet output.
        base_path (str): Base GCS path where data should be stored.
    """
    try:
        logger.info(f"Writing DataFrame to Parquet at {base_path}/{file_name}.")
        output_path = f"{base_path}/{file_name}"
        # write file in parquet in overwrite mode
        # Snappy compression for efficient storage
        # and faster read/write with Parquet
        df.write.mode("overwrite").option("compression", "snappy").parquet(output_path)
    except Exception as e:
        logger.error(f"Error writing DataFrame to Parquet: {e}", exc_info=True)
        raise
    else:
        logger.info(
            f"DataFrame successfully written to {output_path} in Parquet format."
        )


if __name__ == "__main__":
    pass
