from pyspark.sql.functions import col, broadcast, sum as _sum, round
from src.utils.logger import setup_logger

logger = setup_logger("individual_contribution", "logs/aggregation.log")

def join_ConByInd_CommMast_CanComLink_CandMast_df(
    ConByInd_df, CommMast_df, CanComLink_df, CandMast_df
):
    try:
        # Dropping CAND_ID since it's not a reliable column as 'UNLINKED' coming 11568 times
        # Also it leads to duplicate column issue after joining
        CommMast_df = CommMast_df.drop("CAND_ID")
        CommMast_df = CommMast_df.drop("IS_CAND_LINKED")

        # Dropping CMTE_DSGN (Committee designation) to avoid ambiguous reference error
        CommMast_df = CommMast_df.drop("CMTE_DSGN")
        # Dropping IS_AUTH_CMTE as well as will filter on CMTE_DSGN after joining
        CommMast_df = CommMast_df.drop("IS_AUTH_CMTE")

        # Join ConByInd_df , CommMast_df, CanComLink_df, CandMast_df
        ConByInd_CommMast_df = ConByInd_df.join(
            broadcast(CommMast_df), on="CMTE_ID", how="LEFT"
        )
        ConByInd_CommMast_CanComLink_df = ConByInd_CommMast_df.join(
            broadcast(CanComLink_df), on="CMTE_ID", how="LEFT"
        )
        ConByInd_CommMast_CanComLink_CandMast_df = ConByInd_CommMast_CanComLink_df.join(
            broadcast(CandMast_df), on="CAND_ID", how="LEFT"
        )
        logger.info(
            "Successfully joined ConByInd, CommMast, CanComLink, and CandMast dataframes."
        )
        return ConByInd_CommMast_CanComLink_CandMast_df
    except Exception as e:
        logger.error(
            "Error occurred while joining ConByInd, CommMast, CanComLink, and CandMast dataframes: %s",
            str(e),
        )
        raise e


def clean_joined_data(ConByInd_CommMast_CanComLink_CandMast_df):
    try:
        # Drop Null Candidate ID's
        ConByInd_CommMast_CanComLink_CandMast_df = (
            ConByInd_CommMast_CanComLink_CandMast_df.filter(col("CAND_ID").isNotNull())
        )

        # Filterig Valid Entities and non missing state
        valid_entities = ["IND"]
        ConByInd_CommMast_CanComLink_CandMast_df = (
            ConByInd_CommMast_CanComLink_CandMast_df.filter(
                col("ENTITY_TP").isin(valid_entities) & (col("STATE") != "NA")
            )
        )

        # Filter CMTE_DSGN = 'P' or 'A' (principal or authorized)
        # to ensure only authorized committees are counted
        ConByInd_CommMast_CanComLink_CandMast_df = (
            ConByInd_CommMast_CanComLink_CandMast_df.filter(
                col("CMTE_DSGN").isin(["P", "A"])
            )
        )
        logger.info(
            "Successfully cleaned joined data by filtering valid entities and authorized committees."
        )
        return ConByInd_CommMast_CanComLink_CandMast_df
    except Exception as e:
        logger.error(
            "Error occurred while cleaning joined data: %s", str(e)
        )
        raise e


def indiv_contr_per_cand(ConByInd_CommMast_CanComLink_CandMast_df):
    try:
        # Calculating total individual contributions per candidate in million
        TotalIndivConPerCan_df = (
            ConByInd_CommMast_CanComLink_CandMast_df.groupBy(
                "CAND_ID", "CAND_NAME", "CAND_PTY_AFFILIATION", "CAND_OFFICE"
            )
            .agg(_sum("TRANSACTION_AMT").alias("total_indiv_contributions"))
            .orderBy(col("total_indiv_contributions").desc())
            .withColumn(
                "total_indiv_contributions_million",
                round(col("total_indiv_contributions") / 1e6, 2),
            )
        )
        logger.info(
            "Successfully calculated total individual contributions per candidate."
        )
        return TotalIndivConPerCan_df
    except Exception as e:
        logger.error(
            "Error occurred while calculating individual contributions per candidate: %s",
            str(e),
        )
        raise e


if __name__ == "__main__":
    pass
