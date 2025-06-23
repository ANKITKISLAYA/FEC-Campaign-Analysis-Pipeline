from src.aggregation.aggregate_donor import agg_donations_by_state, map_state_code
from src.aggregation.donor_pattern import (
    preprocess_for_donor_analysis,
    biggest_donor,
    donation_distribution,
)
from src.aggregation.individual_contribution import (
    join_ConByInd_CommMast_CanComLink_CandMast_df,
    clean_joined_data,
    indiv_contr_per_cand,
)
from src.aggregation.spending_pattern import (
    filter_OpEx,
    total_expen_month_wise,
)
from src.aggregation.save_data import write_df_to_parquet

from src.utils.load_config import load_config
from src.utils.spark_session import spark_session
from src.utils.logger import setup_logger

logger = setup_logger("aggregation_run", "logs/aggregation.log")


def get_donations_by_cand_state(ConByInd_df, CommMast_df, CanComLink_df, CandMast_df):
    try:

        # Join dataframes
        ConByInd_CommMast_CanComLink_CandMast_df = (
            join_ConByInd_CommMast_CanComLink_CandMast_df(
                ConByInd_df, CommMast_df, CanComLink_df, CandMast_df
            )
        )

        # Clean joined df
        ConByInd_CommMast_CanComLink_CandMast_df = clean_joined_data(
            ConByInd_CommMast_CanComLink_CandMast_df
        )

        # Cache since joined df is getting resused
        ConByInd_CommMast_CanComLink_CandMast_df.cache()
        # Materialize cache
        ConByInd_CommMast_CanComLink_CandMast_df.count()

        # Total individual contributions per candidate
        TotalIndivConPerCan_df = indiv_contr_per_cand(
            ConByInd_CommMast_CanComLink_CandMast_df
        )

        AggDonByState_df = agg_donations_by_state(
            ConByInd_CommMast_CanComLink_CandMast_df
        )
        AggDonByState_df = map_state_code(AggDonByState_df)
        logger.info("Donations by candidate and state aggregated successfully.")
        return TotalIndivConPerCan_df, AggDonByState_df
    except Exception as e:
        logger.error(f"Error in get_donations_by_cand_state: {e}")
        raise e
    finally:
        # Unpersist to free memory
        if "ConByInd_CommMast_CanComLink_CandMast_df" in locals():
            if ConByInd_CommMast_CanComLink_CandMast_df is not None:
                ConByInd_CommMast_CanComLink_CandMast_df.unpersist()
                logger.info(
                    "Unpersisting ConByInd_CommMast_CanComLink_CandMast_df to free memory."
                )


def total_expen_by_month(OpEx_df):
    try:
        # Filter operating expenditures
        OpEx_df_filter = filter_OpEx(OpEx_df)

        # Total expenditure per month
        TotExpPerMon_df = total_expen_month_wise(OpEx_df_filter)
        logger.info("Total expenditure per month calculated successfully.")
        return TotExpPerMon_df
    except Exception as e:
        logger.error(f"Error in total_expen_by_month: {e}")
        raise


def donor_analysis(ConByInd_df):
    try:
        # Caching since preprocess_df used two times
        preprocess_df = preprocess_for_donor_analysis(ConByInd_df).cache()

        # Get biggest donors
        TopDonors_df = biggest_donor(preprocess_df, top_n=20)

        # Get donation distribution
        DonDist_df = donation_distribution(preprocess_df)

        logger.info("Donor analysis completed successfully.")
        return TopDonors_df, DonDist_df
    except Exception as e:
        logger.error(f"Error in donor_analysis: {e}")
        raise
    finally:
        if "preprocess_df" in locals() and preprocess_df is not None:
            # Unpersist to free memory
            preprocess_df.unpersist()
            logger.info("Unpersisted preprocess_df to free memory.")


def run_aggregation_pipeline():
    """
    Main function to run the aggregation pipeline.
    """
    logger.info("Starting aggregation pipeline...")
    try:
        # Load configuration and create Spark session
        config_yml = load_config("config/pipeline_config.yaml")
        spark = spark_session(config_yml, app_name="DataIntegrationAndAggregation")

        # Setting silver data directory path
        silver_path = config_yml["silver_path"]
        # Setting gold data directory path
        gold_path = config_yml["gold_path"]

        # Path to file stored in silver layer
        base_dir_path = silver_path

        # Read Data from parquet

        # Candidate-committee linkages
        CanComLink_df = spark.read.parquet(base_dir_path + "CanComLink_df")

        # Contributions by individuals
        ConByInd_df = spark.read.parquet(base_dir_path + "ConByInd_df")

        # Operating expenditures
        OpEx_df = spark.read.parquet(base_dir_path + "OpEx_df")

        # Candidate master
        CandMast_df = spark.read.parquet(base_dir_path + "CandMast_df")

        # Commitee master
        CommMast_df = spark.read.parquet(base_dir_path + "CommMast_df")

        # Run aggregation for donations by candidate and state
        TotalIndivConPerCan_df, AggDonByState_df = get_donations_by_cand_state(
            ConByInd_df, CommMast_df, CanComLink_df, CandMast_df
        )

        # Run total expenditure by month
        TotExpPerMon_df = total_expen_by_month(OpEx_df)

        # Run donor analysis
        TopDonors_df, DonDist_df = donor_analysis(ConByInd_df)

        # Write aggregated data to Parquet
        write_df_to_parquet(TotalIndivConPerCan_df, "TotalIndivConPerCan_df", gold_path)
        write_df_to_parquet(AggDonByState_df, "AggDonByState_df", gold_path)
        write_df_to_parquet(TotExpPerMon_df, "TotExpPerMon_df", gold_path)
        write_df_to_parquet(TopDonors_df, "TopDonors_df", gold_path)
        write_df_to_parquet(DonDist_df, "DonDist_df", gold_path)
        logger.info("Aggregation pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in run_aggregation_pipeline: {e}")
        raise e
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    try:
        run_aggregation_pipeline()
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise e
