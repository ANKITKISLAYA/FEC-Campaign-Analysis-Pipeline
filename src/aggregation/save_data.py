from src.utils.logger import setup_logger

logger = setup_logger("aggregation_save_data", "logs/aggregation.log")

def write_df_to_parquet(df, file_name, base_path="gs://dataproc-staging-us-central1-40371648517-ndvgfbwp/notebooks/jupyter/FEC-Campaign-Analysis/FEC-Data/gold"):
    """
    Writes a Spark DataFrame to the specified GCS path in Parquet format.

    Args:
        df (DataFrame): Spark DataFrame to write.
        file_name (str): Folder name (like table name) for Parquet output.
        base_path (str): Base GCS path where data should be stored.
    """
    try:
        logger.info(f"Writing DataFrame to Parquet: {file_name}")
        output_path = f"{base_path}/{file_name}"
        df.write.mode("overwrite").parquet(output_path)
        logger.info(f"Data written to: {output_path}")
    except Exception as e:
        logger.error(f"Error writing DataFrame to Parquet: {str(e)}")
        raise e
