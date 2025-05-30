from pyspark.sql.functions import col, when
from src.utils.logger import setup_logger

logger = setup_logger("cancomlink_features", "logs/processing.log")


def features_CanComLink_df(df):
    try:
        # Add committee type descriptions
        df = df.withColumn(
            "CMTE_TP_DESC",
            when(col("CMTE_TP") == "P", "Presidential")
            .when(col("CMTE_TP") == "H", "House")
            .when(col("CMTE_TP") == "S", "Senate")
            .when(col("CMTE_TP") == "X", "Independent Expenditure")
            .otherwise("Other"),
        )
        # Add committee designation categories (P=Principal, A=Authorized, etc.)
        df = df.withColumn(
            "CMTE_DSGN_DESC",
            when(col("CMTE_DSGN") == "P", "Principal Campaign Committee")
            .when(col("CMTE_DSGN") == "A", "Authorized Committee")
            .when(col("CMTE_DSGN") == "J", "Joint Fundraiser")
            .when(col("CMTE_DSGN") == "U", "Unauthorized")
            .otherwise("Other"),
        )
        logger.info("CanComLink features added successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during CanComLink features extraction: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
