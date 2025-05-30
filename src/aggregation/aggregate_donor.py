from pyspark.sql.functions import col, count, sum as _sum, avg, round, udf
from pyspark.sql.types import StringType
from src.utils.logger import setup_logger

logger = setup_logger("aggregate_donor", "logs/aggregation.log")

def agg_donations_by_state(ConByInd_CommMast_CanComLink_CandMast_df):
    try:
        # Total donations
        total_donation = ConByInd_CommMast_CanComLink_CandMast_df.agg(
            _sum("TRANSACTION_AMT")
        ).collect()[0][0]

        # Average donations statewise
        # Total donations statewise
        # Total donations count statewise
        # Filter out unkown states ZZ, AE, AP ('~' means negated logic i.e Not In here)
        # Filter total donations count with greater than 25000
        # Order by total donations desc

        AggDonByState_df = (
            ConByInd_CommMast_CanComLink_CandMast_df.groupBy("STATE")
            .agg(
                round(avg("TRANSACTION_AMT"), 2).alias("avg_donation"),
                count("*").alias("total_donation_count"),
                _sum("TRANSACTION_AMT").alias("total_donation_in_million"),
            )
            .withColumn(
                "total_donation_in_million",
                round(col("total_donation_in_million") / 1e6, 2),
            )
            .withColumn(
                "percent_of_total_donation",
                round(
                    (col("total_donation_in_million") * 1_000_000 / total_donation) * 100, 2
                ),
            )
            .filter(~col("STATE").isin("ZZ", "AE", "AP", "AA"))
            .filter(col("total_donation_count") > 25000)
            .orderBy(col("total_donation_in_million").desc())
        )
        logger.info("Aggregated donations by state successfully.")

        return AggDonByState_df
    except Exception as e:
        logger.error("Error occurred while aggregating donations by state: %s", str(e))
        raise e


def map_state_code(df):
    try:
        # Map of U.S. State Codes to Full Names
        state_code_to_name = {
            "AL": "Alabama",
            "AK": "Alaska",
            "AS": "American Samoa",
            "AZ": "Arizona",
            "AR": "Arkansas",
            "CA": "California",
            "CO": "Colorado",
            "CT": "Connecticut",
            "DE": "Delaware",
            "DC": "District of Columbia",
            "FL": "Florida",
            "GA": "Georgia",
            "GU": "Guam",
            "HI": "Hawaii",
            "ID": "Idaho",
            "IL": "Illinois",
            "IN": "Indiana",
            "IA": "Iowa",
            "KS": "Kansas",
            "KY": "Kentucky",
            "LA": "Louisiana",
            "ME": "Maine",
            "MD": "Maryland",
            "MA": "Massachusetts",
            "MI": "Michigan",
            "MN": "Minnesota",
            "MS": "Mississippi",
            "MO": "Missouri",
            "MT": "Montana",
            "NE": "Nebraska",
            "NV": "Nevada",
            "NH": "New Hampshire",
            "NJ": "New Jersey",
            "NM": "New Mexico",
            "NY": "New York",
            "NC": "North Carolina",
            "ND": "North Dakota",
            "OH": "Ohio",
            "OK": "Oklahoma",
            "OR": "Oregon",
            "PA": "Pennsylvania",
            "PR": "Puerto Rico",
            "RI": "Rhode Island",
            "SC": "South Carolina",
            "SD": "South Dakota",
            "TN": "Tennessee",
            "TX": "Texas",
            "UT": "Utah",
            "VT": "Vermont",
            "VI": "U.S. Virgin Islands",
            "VA": "Virginia",
            "WA": "Washington",
            "WV": "West Virginia",
            "WI": "Wisconsin",
            "WY": "Wyoming",
            "AA": "Armed Forces Americas",
            "AE": "Armed Forces Europe",
            "AP": "Armed Forces Pacific",
            "FM": "Federated States of Micronesia",
            "MP": "Northern Mariana Islands",
            "PW": "Palau",
            "ZZ": "Unknown",
            "NA": "Not Available",
            "ON": "Ontario",
            "OF": "Other Foreign",
        }

        # UDF to get full state name
        def get_state_name(code):
            return state_code_to_name.get(code, "Unknown")

        state_name_udf = udf(get_state_name, StringType())

        # Add a new column with full state names
        enriched_df = df.withColumn("state_name", state_name_udf(col("STATE")))
        logger.info("Mapped state codes to full names successfully.")
        return enriched_df
    except Exception as e:
        logger.error("Error occurred while mapping state codes: %s", str(e))
        raise e


if __name__ == "__main__":
    pass
