def write_df_to_parquet(df, file_name, base_path="gs://dataproc-staging-us-central1-784600309852-sdhxiysx/notebooks/jupyter/FEC Project/data/aggregated/2019-2020"):
    """
    Writes a Spark DataFrame to the specified GCS path in Parquet format.

    Args:
        df (DataFrame): Spark DataFrame to write.
        file_name (str): Folder name (like table name) for Parquet output.
        base_path (str): Base GCS path where data should be stored.
    """
    output_path = f"{base_path}/{file_name}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data written to: {output_path}")

if __name__ == "__main__":
    write_df_to_parquet()