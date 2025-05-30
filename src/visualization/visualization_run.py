from src.utils.spark_session import spark_session
from src.utils.logger import setup_logger
from src.utils.load_config import load_config

from src.visualization.donationanalysis_plot import (
    donation_by_state_plot,
    total_donations_by_state_plot,
    average_donation_by_state_plot,
    percentage_donation_by_state_plot,
)
from src.visualization.donorsanalysis_plot import (
    top20_donors_plot,
    donation_distribution_plot,
)
from src.visualization.individualcontributions_plot import (
    individual_contributions_plot,
    contributions_by_party_plot,
)
from src.visualization.totalexpendbymonth_plot import (
    monthly_expenditures_trend_plot,
    monthly_expenditures_comparison_plot,
)


logger = setup_logger("visualization_run", "logs/visualization.log")


def run_visualization():
    try:

        # Load configuration and create Spark session
        config_yml = load_config("config/pipeline_config.yaml")
        spark = spark_session(config_yml, app_name="DataVisualization")

        # Setting aggregated data directory path
        aggregated_data_path = config_yml["aggregated_data_path"]
        base_dir_path = aggregated_data_path

        # Read Data from parquet

        # individual contributions per candidate
        TotalIndivConPerCan_df = spark.read.parquet(
            base_dir_path + "TotalIndivConPerCan_df"
        )

        # aggregated donations grouped by state
        AggDonByState_df = spark.read.parquet(base_dir_path + "AggDonByState_df")

        # campaign expenditures grouped by month
        TotExpPerMon_df = spark.read.parquet(base_dir_path + "TotExpPerMon_df")

        # individual donors by total amount and frequency
        TopDonors_df = spark.read.parquet(base_dir_path + "TopDonors_df")

        # donation distribution by amount buckets
        DonDist_df = spark.read.parquet(base_dir_path + "DonDist_df")

        # Run individual contributions plot
        individual_contributions_plot(TotalIndivConPerCan_df)
        contributions_by_party_plot(TotalIndivConPerCan_df)

        # Run donation analysis plots
        donation_by_state_plot(AggDonByState_df)
        total_donations_by_state_plot(AggDonByState_df)
        average_donation_by_state_plot(AggDonByState_df)
        percentage_donation_by_state_plot(AggDonByState_df)

        # Run donors analysis plots
        top20_donors_plot(TopDonors_df)
        donation_distribution_plot(DonDist_df)

        # Run total expenditures by month plots
        monthly_expenditures_trend_plot(TotExpPerMon_df)
        monthly_expenditures_comparison_plot(TotExpPerMon_df)

        logger.info("All visualizations completed successfully.")

    except Exception as e:
        logger.error(f"Error in run_visualization: {e}")
        raise e


if __name__ == "__main__":

    try:
        logger.info("Starting visualization run...")
        run_visualization()
        logger.info("Visualization run completed.")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise e
