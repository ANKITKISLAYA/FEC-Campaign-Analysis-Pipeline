from src.utils.logger import setup_logger

logger = setup_logger("gcs_ingestion", "logs/ingestion.log")


def read_data(spark, file_path, header=None, columns=None):
    """
    Reads the data from the given GCS path.

    Parameters:
        spark(object): spark session instance.
        file_path(str): GCS path of raw data.
        header(str): header path if present.
        columns(list): provide column name as list
    """
    try:
        logger.info(f"Reading data from {file_path}:")

        if header is not None:
            # Take column from header file in list and create df
            header_path = header
            headers_df = spark.read.option("header", "true").csv(header_path)
            headers = headers_df.columns
            df = (
                spark.read.option("delimiter", "|")
                .option("header", "false")
                .csv(file_path)
                .toDF(*headers)
            )
            logger.info("Data read successfully using headers from file.")
            return df

        elif columns is not None:
            df = (
                spark.read.option("delimiter", "|")
                .option("header", "false")
                .csv(file_path)
                .toDF(*columns)
            )
            logger.info("Data read successfully using provided columns.")
            return df

        else:
            logger.info("Neither header nor columns were provided.")
            raise ValueError("Either header or columns list must be provided.")

    except ValueError as ve:
        logger.error(f"ValueError: {ve}", exc_info=True)
        raise

    except Exception as e:
        logger.error(
            f"An error occurred while reading data from {file_path}: {e}", exc_info=True
        )
        raise


if __name__ == "__main__":
    pass
