import os
import matplotlib.pyplot as plt
import seaborn as sns
from src.utils.logger import setup_logger

# create plots directory if it doesn't exist
plot_dir = os.path.join("src", "visualization", "plots")
os.makedirs(plot_dir, exist_ok=True)

logger = setup_logger("total_expend_by_month_plot", "logs/visualization.log")


def monthly_expenditures_trend_plot(TotExpPerMon_df):
    """
    Plots the total campaign expenditures grouped by month.

    Parameters:
    TotExpPerMon_df (DataFrame): DataFrame containing total expenditures per month.
    """
    try:
        # Convert to pandas df
        TotExpPerMon_pdf = TotExpPerMon_df.toPandas()
        # Creating a Month-Year column for timeline plotting
        TotExpPerMon_pdf["MONTH_YEAR"] = (
            TotExpPerMon_pdf["YEAR"].astype(str)
            + "-"
            + TotExpPerMon_pdf["MONTH"].astype(str).str.zfill(2)
        )

        # Plot
        plt.figure(figsize=(14, 6))
        sns.lineplot(
            data=TotExpPerMon_pdf,
            x="MONTH_YEAR",
            y="total_monthly_spending_in_millions",
            marker="o",
            linewidth=2.5,
            color="steelblue",
        )

        plt.xticks(rotation=45)
        plt.xlabel("Month-Year")
        plt.ylabel("Monthly Expenditure (in million $)")
        plt.title("Campaign Monthly Expenditure Trend (2019–2020)")
        plt.tight_layout()
        plt.grid(True)

        # Save plot to file
        output_path = os.path.join(plot_dir, "monthly_expenditures_trend.png")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Plot saved to {output_path}")
    except Exception as e:
        print(f"Error in monthly_expenditures_plot: {e}")
        raise e


def monthly_expenditures_comparison_plot(TotExpPerMon_df):
    """
    Plots the total campaign expenditures grouped by month, comparing across years.

    Parameters:
    TotExpPerMon_df (DataFrame): DataFrame containing total expenditures per month.
    """
    try:
        # Convert to pandas df
        TotExpPerMon_pdf = TotExpPerMon_df.toPandas()

        # Create a new column for month names
        TotExpPerMon_pdf["MONTH_NAME"] = TotExpPerMon_pdf["MONTH"].apply(
            lambda x: f"{x:02d}"
        )

        # Plot
        plt.figure(figsize=(14, 6))
        sns.barplot(
            data=TotExpPerMon_pdf,
            x="MONTH_NAME",
            y="total_monthly_spending_in_millions",
            hue="YEAR",
            palette="Set2",
        )

        plt.xlabel("Month")
        plt.ylabel("Monthly Expenditure (in million $)")
        plt.title("Campaign Expenditure by Month – Comparison Across Years")
        plt.tight_layout()

        # Save plot to file
        output_path = os.path.join(plot_dir, "monthly_expenditures_comparison.png")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Plot saved to {output_path}")
    except Exception as e:
        print(f"Error in monthly_expenditures_comparison_plot: {e}")
        raise e


if __name__ == "__main__":
    pass
