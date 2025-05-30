import pandas as pd
from sqlalchemy import create_engine

from src.utils.logger import setup_logger

logger = setup_logger("mysql_ingestion", "logs/ingestion.log")


def read_table_to_df(table_name, config_yml):
    """
    Reads a table from a MySQL database into a Pandas DataFrame.
    Parameters:
        table_name (str): The name of the table to read.
        config_yml (dict): Configuration dictionary containing MySQL connection details.
    Returns:
        pd.DataFrame: DataFrame containing the data from the specified table.
    """
    try:
        logger.info(f"Reading table {table_name} from MySQL database.")
        # Create SQLAlchemy engine
        engine = create_engine(
            f"mysql+pymysql://{config_yml['mysql_config']['user']}:"
            f"{config_yml['mysql_config']['password']}@"
            f"{config_yml['mysql_config']['host']}:"
            f"{config_yml['mysql_config']['port']}/"
            f"{config_yml['mysql_config']['database']}"
        )

        # Read table into DataFrame
        df_read = pd.read_sql_table(table_name, con=engine)
    except Exception as e:
        logger.error(f"Error reading table {table_name}: {e}", exc_info=True)
        raise
    else:
        logger.info(f"Successfully read table {table_name} into DataFrame.")
        return df_read
    finally:
        # Close the SQLAlchemy engine
        if engine:
            engine.dispose()
            logger.info("SQLAlchemy engine disposed.")


if __name__ == "__main__":
    pass
