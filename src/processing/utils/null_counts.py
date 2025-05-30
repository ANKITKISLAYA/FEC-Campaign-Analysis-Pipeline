from pyspark.sql.functions import col, when, sum as _sum


# Function to check null counts in dataframe
def null_counts(df):
    null_counts_df = df.select(
        [_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    )
    return null_counts_df


if __name__ == "__main__":
    pass
