import os
import matplotlib.pyplot as plt
import seaborn as sns
from src.utils.logger import setup_logger

# create plots directory if it doesn't exist
plot_dir = os.path.join("src", "visualization", "plots")
os.makedirs(plot_dir, exist_ok=True)

logger = setup_logger("donorsanalysis_plot", "logs/visualization.log")


def top20_donors_plot(TopDonors_df):
    """
    Plots the top 20 individual donors by total amount and frequency of donations.

    Parameters:
    TopDonors_df (DataFrame): DataFrame containing top individual donors.
    """
    try:
        # Convert to pandas df
        TopDonors_pdf = TopDonors_df.toPandas()
        TopDonors_pdf

        # Plot
        plt.figure(figsize=(12, 6))
        sns.barplot(x="total_donat_in_million", y="NAME", data=TopDonors_pdf.head(20))

        # Set axis labels and title
        plt.xlabel("Total Donation (in Millions)")  # X-axis label
        plt.ylabel("Donor Name")  # Y-axis label
        plt.title("Top 20 Individual Donors by Total Contributions")

        plt.tight_layout()
        plt.show()

        plt.tight_layout()

        # Save plot to file
        output_path = os.path.join(plot_dir, "top_20_donors.png")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Plot saved to {output_path}")
    except Exception as e:
        print(f"Error in top20_donors_plot: {e}")
        raise e


def donation_distribution_plot(DonDist_df):
    """
    Plots the distribution of donations by amount buckets.

    Parameters:
    DonDist_df (DataFrame): DataFrame containing donation distribution.
    """
    try:
        # convert to pandas df
        DonDist_pdf = DonDist_df.toPandas()
        DonDist_pdf

        # plot
        plt.figure(figsize=(12, 6))
        sns.barplot(
            data=DonDist_pdf,
            x="amount_bucket",
            y="total_amt_in_millions",
            palette="magma",
        )
        plt.xticks(rotation=45)
        plt.xlabel("Donation Amount Bucket ($)")
        plt.ylabel("Total Amount Donated (in million $)")
        plt.title("Total Donation Value by Amount Bucket")
        plt.tight_layout()

        # Save plot to file
        output_path = os.path.join(plot_dir, "donation_distribution.png")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Plot saved to {output_path}")
    except Exception as e:
        print(f"Error in donation_distribution_plot: {e}")
        raise e


if __name__ == "__main__":
    pass
