from pyspark.sql.functions import col, count, sum as _sum, round, when
from src.utils.logger import setup_logger

logger = setup_logger("donor_pattern", "logs/aggregation.log")

def preprocess_for_donor_analysis(df):
    try:
        # Filtering only individual contributors
        # Remove entries where NAME contains
        # organization-like patterns ("ASSOC", "FUND", "NATIONAL", etc.)
        df = df.filter(
            (col("ENTITY_TP") == "IND")
            & (
                ~col("NAME").rlike(
                    "(?i)FUND|ASSOCIATION|NATIONAL|COMMITTEE|COUNCIL|UNION|INC|CORP|LLC"
                )
            )
        )
        logger.info("Filtered individual contributors successfully.")
        return df
    except Exception as e:
        logger.error(
            "Error occurred while preprocessing data for donor analysis: %s", str(e)
        )
        raise e


def biggest_donor(df, top_n=20):
    try:
        # Aggregation by donor name
        df = (
            df.groupBy("NAME")
            .agg(
                round(_sum("TRANSACTION_AMT") / 1e6, 2).alias("total_donat_in_million"),
                count("*").alias("donat_freq"),
            )
            .orderBy(col("total_donat_in_million").desc())
            .limit(top_n)
        )
        logger.info("Calculated biggest donors successfully.")
        return df
    except Exception as e:
        logger.error("Error occurred while calculating biggest donors: %s", str(e))
        raise e


def donation_distribution(df):
    try:
        # Create Buckets for different amount
        df = df.withColumn(
            "amount_bucket",
            when(col("TRANSACTION_AMT") < 100, "<100")
            .when(col("TRANSACTION_AMT") <= 500, "101-500")
            .when(col("TRANSACTION_AMT") <= 1000, "501-1K")
            .when(col("TRANSACTION_AMT") <= 5000, "1K-5K")
            .when(col("TRANSACTION_AMT") <= 10000, "5K-10K")
            .when(col("TRANSACTION_AMT") <= 50000, "10K-50K")
            .when(col("TRANSACTION_AMT") <= 100000, "50K-100K")
            .when(col("TRANSACTION_AMT") <= 500000, "100K-500K")
            .when(col("TRANSACTION_AMT") <= 1000000, "500K-1M")
            .when(col("TRANSACTION_AMT") <= 5000000, "1M-5M")
            .when(col("TRANSACTION_AMT") <= 10000000, "5M-10M")
            .otherwise(">10M"),
        )
        df = (
            df.groupBy("amount_bucket")
            .agg(
                count("*").alias("donation_count"),
                round(_sum("TRANSACTION_AMT") / 1e6, 2).alias("total_amt_in_millions"),
            )
            .orderBy("donation_count")
        )
        logger.info("Calculated donation distribution successfully.")
        return df
    except Exception as e:
        logger.error("Error occurred while calculating donation distribution: %s", str(e))
        raise e


if __name__ == "__main__":
    pass