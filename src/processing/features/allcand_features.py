from pyspark.sql.functions import (
    to_date,
    year,
    month,
    quarter,
    upper,
    col,
    when,
    sum as _sum,
    avg as _avg,
)
from src.utils.logger import setup_logger

logger = setup_logger("allcand_features", "logs/processing.log")


def features_AllCand_df(df):
    try:
        # Extract Year, Month and Quarter from end coverage date and normalise state and district
        df = (
            df.withColumn("CVG_END_DT", to_date("CVG_END_DT", "MM/dd/yyyy"))
            .withColumn("YEAR", year("CVG_END_DT"))
            .withColumn("MONTH", month("CVG_END_DT"))
            .withColumn("QUARTER", quarter("CVG_END_DT"))
            .withColumn("CAND_OFFICE_ST", upper(col("CAND_OFFICE_ST")))
            .withColumn("CAND_OFFICE_DISTRICT", upper(col("CAND_OFFICE_DISTRICT")))
        )

        # Adjusting for intra-committee transfers
        df = df.withColumn(
            "TTL_RECEIPTS", col("TTL_RECEIPTS") - col("TRANS_FROM_AUTH")
        ).withColumn("TTL_DISB", col("TTL_DISB") - col("TRANS_TO_AUTH"))

        # Financial Behavior Feature Engineering
        df = (
            df.withColumn("net_cash_position", col("TTL_RECEIPTS") - col("TTL_DISB"))
            .withColumn("cash_delta", col("COH_COP") - col("COH_BOP"))
            .withColumn(
                "self_sufficiency_ratio",
                when(
                    col("TTL_RECEIPTS") != 0, col("CAND_CONTRIB") / col("TTL_RECEIPTS")
                ).otherwise(0),
            )
            .withColumn(
                "loan_dependence_ratio",
                when(
                    col("TTL_RECEIPTS") != 0,
                    (col("CAND_LOANS") + col("OTHER_LOANS")) / col("TTL_RECEIPTS"),
                ).otherwise(0),
            )
            .withColumn(
                "refund_ratio",
                when(
                    col("TTL_RECEIPTS") != 0,
                    (col("INDIV_REFUNDS") + col("CMTE_REFUNDS")) / col("TTL_RECEIPTS"),
                ).otherwise(0),
            )
            .withColumn(
                "indiv_contrib_ratio",
                when(
                    col("TTL_RECEIPTS") != 0,
                    col("TTL_INDIV_CONTRIB") / col("TTL_RECEIPTS"),
                ).otherwise(0),
            )
        )
        logger.info("AllCand features extraction completed successfully.")
        return df
    except Exception as e:
        logger.error(
            f"An error occurred during AllCand features extraction: {e}",
            exc_info=True,
        )
        raise


def geo_AllCand_df(df):
    """
    Aggregate Data by geography.
    """
    try:
        # Aggregation by candidate state and district
        geo_agg_df = df.groupBy("CAND_OFFICE_ST", "CAND_OFFICE_DISTRICT").agg(
            _sum("TTL_RECEIPTS").alias("total_receipts"),
            _sum("TTL_DISB").alias("total_disbursements"),
            _sum("CAND_CONTRIB").alias("total_candidate_contributions"),
            _sum("CAND_LOANS").alias("total_candidate_loans"),
            _sum("OTHER_LOANS").alias("total_other_loans"),
            _sum("INDIV_REFUNDS").alias("total_indiv_refunds"),
            _sum("CMTE_REFUNDS").alias("total_cmte_refunds"),
            _avg("TTL_RECEIPTS").alias("avg_receipts_per_candidate"),
            _avg("TTL_DISB").alias("avg_disbursements_per_candidate"),
        )
        logger.info("AllCand geo aggregation completed successfully.")
        return geo_agg_df
    except Exception as e:
        logger.error(
            f"An error occurred during AllCand geo aggregation: {e}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    pass
