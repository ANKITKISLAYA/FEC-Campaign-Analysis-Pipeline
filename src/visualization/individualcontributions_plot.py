import os
import matplotlib.pyplot as plt
import seaborn as sns
from src.utils.logger import setup_logger

from pyspark.sql.functions import col, when, round, sum as _sum

# Create plots directory if it doesn't exist
plot_dir = os.path.join("src", "visualization", "plots")
os.makedirs(plot_dir, exist_ok=True)

logger = setup_logger("individual_contributions_plot", "logs/visualization.log")


def individual_contributions_plot(TotalIndivConPerCan_df):
    """
    Plots the total individual contributions per candidate.

    Parameters:
    TotalIndivConPerCan_df (DataFrame): DataFrame containing total individual contributions per candidate.
    """
    try:

        # Convert to pandas df
        TotalIndivConPerCan_pdf = TotalIndivConPerCan_df.toPandas()
        TotalIndivConPerCan_pdf.head(10)

        # Plot
        plt.figure(figsize=(12, 6))
        sns.barplot(
            data=TotalIndivConPerCan_pdf.head(20),
            x="CAND_NAME",
            y="total_indiv_contributions_million",
            hue="CAND_PTY_AFFILIATION",
        )

        # Set axis labels and title
        plt.xlabel("Candidate Name")
        plt.ylabel("Total Contribution (in Million $)")
        plt.title(
            "Top 20 Candidates by Total Individual Contributions (Colored by Party)"
        )

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        # Save plot to file
        output_path = os.path.join(plot_dir, "top_candidates_by_contribution.png")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Plot saved to {output_path}")
    except Exception as e:
        print(f"Error in individual_contributions_plot: {e}")
        raise e


def contributions_by_party_plot(TotalIndivConPerCan_df):
    """
    Plots contributions by party.

    Parameters:
    TotalIndivConPerCan_df (DataFrame): DataFrame containing contributions by party.
    """
    try:

        # Add a new column to group parties
        TotalIndivConPerCan_df_grouped = TotalIndivConPerCan_df.withColumn(
            "party_group",
            when(col("CAND_PTY_AFFILIATION") == "REP", "Republican")
            .when(col("CAND_PTY_AFFILIATION") == "DEM", "Democrat")
            .otherwise("Others"),
        )

        # Combined contributions for all parties
        TotalContributions = TotalIndivConPerCan_df_grouped.agg(
            _sum("total_indiv_contributions")
        ).collect()[0][0]

        # Calculating contributions by party and it's share by percentage
        ConByParty_df = TotalIndivConPerCan_df_grouped.groupBy("party_group").agg(
            _sum(col("total_indiv_contributions")).alias("contr_by_party")
        )
        ConByParty_df = ConByParty_df.withColumn(
            "per_contr_by_party",
            round((col("contr_by_party") / TotalContributions) * 100, 2),
        )

        ConByParty_pdf = ConByParty_df.toPandas()

        # Pie chart
        plt.figure(figsize=(6, 6))
        color_map = {"Republican": "red", "Democrat": "blue", "Others": "gray"}
        colors = [color_map[party] for party in ConByParty_pdf["party_group"]]

        plt.pie(
            ConByParty_pdf["per_contr_by_party"],
            labels=ConByParty_pdf["party_group"],
            autopct="%1.1f%%",
            colors=colors,
            startangle=90,
            textprops={"fontsize": 12},
        )

        plt.title("Contribution Share by Party (Including Others)", fontsize=14)
        plt.tight_layout()

        # Save plot to file
        output_path = os.path.join(plot_dir, "contributions_by_party.png")
        plt.savefig(output_path)
        plt.close()

        logger.info(f"Plot saved to {output_path}")

    except Exception as e:
        print(f"Error in contributions_by_party_plot: {e}")
        raise e


if __name__ == "__main__":
    pass
