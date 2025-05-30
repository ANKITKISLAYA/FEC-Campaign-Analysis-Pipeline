from pyspark.sql.functions import upper, col, sum as _sum, countDistinct
from src.utils.logger import setup_logger

logger = setup_logger("tranonecomtoano_features", "logs/processing.log")


def features_TranOneComToAno_df(df):
    try:
        # Normalize
        df = (
            df.withColumn("TRANSACTION_TP", upper(col("TRANSACTION_TP")))
            .withColumn("ENTITY_TP", upper(col("ENTITY_TP")))
            .withColumn("STATE", upper(col("STATE")))
        )

        # Total transaction amount by state
        agg_by_state = df.groupBy("STATE").agg(
            _sum("TRANSACTION_AMT").alias("TOTAL_AMT_STATE")
        )

        # Total contributions by entity type
        agg_by_entity = df.groupBy("ENTITY_TP").agg(
            _sum("TRANSACTION_AMT").alias("TOTAL_AMT_ENTITY"),
            countDistinct("NAME").alias("UNIQUE_CONTRIBUTORS"),
        )
        logger.info("TranOneComToAno features added successfully.")
        return df, agg_by_state, agg_by_entity
    except Exception as e:
        logger.error(
            f"An error occurred during TranOneComToAno features extraction: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
