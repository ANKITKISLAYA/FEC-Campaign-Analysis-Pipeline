from pyspark.sql.functions import col, sum as _sum, round, when
from src.utils.logger import setup_logger

logger = setup_logger("spending_pattern", "logs/aggregation.log")

def filter_OpEx(OpEx_df):
    try:
        OpEx_df = OpEx_df.filter(
            (col("TRANSACTION_AMT") > 0)  # Only consider positive spending
            & (
                col("AMNDT_IND").isin("N", "T")
            )  # Only original (N) or termination (T) filings
            & (col("SCHED_TP_CD") == "SB")  # Schedule B: Itemized Disbursements
            & (
                (col("YEAR") == 2019)  # Include full year 2019
                | (
                    (col("YEAR") == 2020) & (col("MONTH") <= 12)
                )  # Include up to Oct 2020 (before election which is in November)
            )
        )
        logger.info("Filtered OpEx DataFrame successfully.")
        return OpEx_df
    except Exception as e:
        logger.error(f"Error filtering OpEx DataFrame: {e}", exc_info=True)
        raise


def total_expen_month_wise(OpEx_df):
    try:
        # Group by Year and Month to calculate total expenditure
        OpEx_df = (
            OpEx_df.groupBy("YEAR", "MONTH")
            .agg(
                round(_sum("TRANSACTION_AMT") / 1e6, 2).alias(
                    "total_monthly_spending_in_millions"
                )
            )
            .orderBy("YEAR", "MONTH")
        )

        # mapping month numbers to names
        TotExpPerMon_df = OpEx_df.withColumn(
            "MONTH_NAME",
            when(col("MONTH") == 1, "Jan")
            .when(col("MONTH") == 2, "Feb")
            .when(col("MONTH") == 3, "Mar")
            .when(col("MONTH") == 4, "Apr")
            .when(col("MONTH") == 5, "May")
            .when(col("MONTH") == 6, "Jun")
            .when(col("MONTH") == 7, "Jul")
            .when(col("MONTH") == 8, "Aug")
            .when(col("MONTH") == 9, "Sep")
            .when(col("MONTH") == 10, "Oct")
            .when(col("MONTH") == 11, "Nov")
            .when(col("MONTH") == 12, "Dec")
            .otherwise("Unknown"),
        )
        return TotExpPerMon_df
    except Exception as e:
        logger.error(f"Error calculating total expenditure month-wise: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    pass
