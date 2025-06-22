import os
import matplotlib.pyplot as plt
import seaborn as sns
from src.utils.logger import setup_logger

# Create plots directory if it doesn't exist
plot_dir = os.path.join("src", "visualization", "plots")
os.makedirs(plot_dir, exist_ok=True)

logger = setup_logger("donation_analysis_plot", "logs/visualization.log")


def donation_by_state_plot(AggDonByState_df):
    """
    Plots the total donations by state.

    Parameters:
    AggDonByState_df (DataFrame): DataFrame containing aggregated donations by state.
    """
    try:
        # Convert to pandas df
        AggDonByState_pdf = AggDonByState_df.toPandas()
        AggDonByState_pdf.head(10)

        # Plot
        plt.figure(figsize=(12, 6))
        sns.barplot(
            data=AggDonByState_pdf,
            x="state_name",
            y="total_donation_in_million",
            palette="viridis",
        )

        # Set axis labels and title
        plt.xlabel("State")
        plt.ylabel("Total Donations (in Million $)")
        plt.title("Total Donations by State")

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        # Save plot to file
        output_path = os.path.join(plot_dir, "donations_by_state.png")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Plot saved to {output_path}")
    except Exception as e:
        print(f"Error in donation_by_state_plot: {e}")
        raise e


def total_donations_by_state_plot(AggDonByState_df):
    """
    Plots the total donations by state.

    Parameters:
    AggDonByState_df (DataFrame): DataFrame containing aggregated donations by state.
    """
    try:
        # Convert to pandas df
        AggDonByState_pdf = AggDonByState_df.toPandas()
        AggDonByState_pdf.head(10)

        # Plot
        plt.figure(figsize=(12, 6))
        sns.barplot(
            data=AggDonByState_pdf,
            x="state_name",
            y="total_donation_in_million",
            palette="viridis",
        )

        # Set axis labels and title
        plt.xlabel("State")
        plt.ylabel("Total Donations (in Million $)")
        plt.title("Total Donations by State")

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        # Save plot to file
        output_path = os.path.join(plot_dir, "total_donations_by_state.png")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Plot saved to {output_path}")
    except Exception as e:
        print(f"Error in total_donations_by_state_plot: {e}")
        raise e


def average_donation_by_state_plot(AggDonByState_df):
    """
    Plots the average donation by state.

    Parameters:
    AggDonByState_df (DataFrame): DataFrame containing aggregated donations by state.
    """
    try:
        # Convert to pandas df
        AggDonByState_pdf = AggDonByState_df.toPandas()
        AggDonByState_pdf.head(10)

        # Sort by average donation
        AggDonByState_pdf_sorted = AggDonByState_pdf.sort_values(
            by="avg_donation", ascending=False
        )

        # Plot
        plt.figure(figsize=(12, 6))
        sns.barplot(
            data=AggDonByState_pdf_sorted,
            x="state_name",
            y="avg_donation",
            palette="mako",
        )

        # Set axis labels and title
        plt.xlabel("State")
        plt.ylabel("Average Donation ($)")
        plt.title("Average Individual Donation by State")

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        # Save plot to file
        output_path = os.path.join(plot_dir, "average_donation_by_state.png")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Plot saved to {output_path}")
    except Exception as e:
        print(f"Error in average_donation_by_state_plot: {e}")
        raise e


def percentage_donation_by_state_plot(AggDonByState_df):
    """
    Plots the percentage of total donations by state.

    Parameters:
    AggDonByState_df (DataFrame): DataFrame containing aggregated donations by state.
    """
    try:
        # Convert to pandas df
        AggDonByState_pdf = AggDonByState_df.toPandas()
        AggDonByState_pdf.head(10)

        # Calculate percentage of total donations
        total_donations = AggDonByState_pdf["total_donation_in_million"].sum()
        AggDonByState_pdf["percentage"] = (
            AggDonByState_pdf["total_donation_in_million"] / total_donations
        ) * 100

        # Plot
        plt.figure(figsize=(10, 8))
        sns.barplot(
            data=AggDonByState_pdf,
            x="percent_of_total_donation",
            y="state_name",
            palette="coolwarm",
        )
        plt.xlabel("Percentage of Total Donations (%)")
        plt.ylabel("State")
        plt.title("State-wise Share in Total Contributions")
        plt.tight_layout()

        # Save plot to file
        output_path = os.path.join(plot_dir, "percentage_donation_by_state.png")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Plot saved to {output_path}")
    except Exception as e:
        print(f"Error in percentage_donation_by_state_plot: {e}")
        raise e


if __name__ == "__main__":
    pass
