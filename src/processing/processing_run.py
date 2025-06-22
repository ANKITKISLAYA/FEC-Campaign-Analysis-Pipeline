from src.processing.cleaning import (
    allcand_clean,
    tranonecomtoano_clean,
    pacsum_clean,
    cancomlink_clean,
    candmast_clean,
    commmast_clean,
    conbyind_clean,
    confromcomtocanindexpen_clean,
    housencurcam_clean,
    base_clean,
    opex_clean,
)

from src.processing.typecasting import (
    allcand_typecast,
    tranonecomtoano_typecast,
    pacsum_typecast,
    cancomlink_typecast,
    candmast_typecast,
    conbyind_typecast,
    confromcomtocanindexpen_typecast,
    housencurcam_typecast,
    opex_typecast,
)

from src.processing.features import (
    pacsum_features,
    tranonecomtoano_features,
    allcand_features,
    cancomlink_features,
    candmast_features,
    commmast_features,
    conbyind_features,
    confromcomtocanindexpen_features,
    housencurcam_features,
    opex_features,
)
from src.processing.utils.write_parquet import write_df_to_parquet

from src.utils.load_config import load_config
from src.utils.spark_session import spark_session
from src.utils.logger import setup_logger

logger = setup_logger("processing_run", "logs/processing.log")


def AllCand_df_processing(AllCand_df, silver_path):
    try:
        # Clean data
        AllCand_df = base_clean.base_clean(AllCand_df)
        AllCand_df = allcand_clean.clean_AllCand_df(AllCand_df)

        # Type cast
        AllCand_df = allcand_typecast.typecast_AllCand_df(AllCand_df)

        # Feature Engineering
        AllCand_df = allcand_features.features_AllCand_df(AllCand_df)

        # Cache since AllCand_df reused in aggregation
        AllCand_df = AllCand_df.cache()
        logger.info("AllCand_df cached")

        # Derived AllCand_geoAgg_df using cached df above
        AllCand_geoAgg_df = allcand_features.geo_AllCand_df(AllCand_df)

        # Save Data in processed Layer
        write_df_to_parquet(AllCand_df, "AllCand_df", silver_path)
        write_df_to_parquet(AllCand_geoAgg_df, "AllCand_geoAgg_df", silver_path)

        # Uncache after writing to free memory
        AllCand_df.unpersist()
        logger.info("AllCand_df uncached")
        logger.info("AllCand_df processing completed successfully.")

    except Exception as e:
        logger.error(
            f"An error occurred during AllCand_df processing: {e}", exc_info=True
        )
        raise


def TranOneComToAno_df_processing(TranOneComToAno_df, silver_path):
    try:
        # Clean data
        TranOneComToAno_df = base_clean.base_clean(TranOneComToAno_df)
        TranOneComToAno_df = tranonecomtoano_clean.clean_TranOneComToAno_df(
            TranOneComToAno_df
        )

        # Type cast
        TranOneComToAno_df = tranonecomtoano_typecast.typecast_TranOneComToAno_df(
            TranOneComToAno_df
        )

        # Feature Engineering
        TranOneComToAno_df, agg_by_state, agg_by_entity = (
            tranonecomtoano_features.features_TranOneComToAno_df(TranOneComToAno_df)
        )

        # Save Data in processed Layer
        write_df_to_parquet(
            TranOneComToAno_df, "TranOneComToAno_df", silver_path
        )

        logger.info("TranOneComToAno_df processing completed successfully.")

    except Exception as e:
        logger.error(
            f"An error occurred during TranOneComToAno_df processing: {e}",
            exc_info=True,
        )
        raise


def CanComLink_df_processing(CanComLink_df, silver_path):

    try:
        # Clean data
        CanComLink_df = base_clean.base_clean(CanComLink_df)
        CanComLink_df = cancomlink_clean.clean_CanComLink_df(CanComLink_df)

        # Type cast
        CanComLink_df = cancomlink_typecast.typecast_CanComLink_df(CanComLink_df)

        # Feature Engineering
        CanComLink_df = cancomlink_features.features_CanComLink_df(CanComLink_df)

        # Save Data in processed Layer
        write_df_to_parquet(CanComLink_df, "CanComLink_df", silver_path)

        logger.info("CanComLink_df processing completed successfully.")

    except Exception as e:
        logger.error(
            f"An error occurred during CanComLink_df processing: {e}", exc_info=True
        )
        raise


def ConByInd_df_processing(ConByInd_df, silver_path):
    try:
        # Clean data
        ConByInd_df = base_clean.base_clean(ConByInd_df)
        ConByInd_df = conbyind_clean.clean_ConByInd_df(ConByInd_df)

        # Type cast
        ConByInd_df = conbyind_typecast.typecast_ConByInd_df(ConByInd_df)

        # Feature Engineering
        ConByInd_df = conbyind_features.features_ConByInd_df(ConByInd_df)

        # Save Data in processed Layer
        write_df_to_parquet(ConByInd_df, "ConByInd_df", silver_path)

        logger.info("ConByInd_df processing completed successfully.")

    except Exception as e:
        logger.error(
            f"An error occurred during ConByInd_df processing: {e}", exc_info=True
        )
        raise


def ConFromComToCanIndExpen_df_processing(
    ConFromComToCanIndExpen_df, silver_path
):
    try:
        # Clean data
        ConFromComToCanIndExpen_df = base_clean.base_clean(ConFromComToCanIndExpen_df)

        ConFromComToCanIndExpen_df = (
            confromcomtocanindexpen_clean.clean_ConFromComToCanIndExpen_df(
                ConFromComToCanIndExpen_df
            )
        )

        # Type cast
        ConFromComToCanIndExpen_df = (
            confromcomtocanindexpen_typecast.typecast_ConFromComToCanIndExpen_df(
                ConFromComToCanIndExpen_df
            )
        )

        # Feature Engineering
        ConFromComToCanIndExpen_df = (
            confromcomtocanindexpen_features.features_ConFromComToCanIndExpen_df(
                ConFromComToCanIndExpen_df
            )
        )

        # Save Data in processed Layer
        write_df_to_parquet(
            ConFromComToCanIndExpen_df,
            "ConFromComToCanIndExpen_df",
            silver_path,
        )

        logger.info("ConFromComToCanIndExpen_df processing completed successfully.")

    except Exception as e:
        logger.error(
            f"An error occurred during ConFromComToCanIndExpen_df processing: {e}",
            exc_info=True,
        )
        raise


def HouSenCurCam_df_processing(HouSenCurCam_df, silver_path):
    try:
        # Clean data
        HouSenCurCam_df = base_clean.base_clean(HouSenCurCam_df)
        HouSenCurCam_df = housencurcam_clean.clean_HouSenCurCam_df(HouSenCurCam_df)

        # Type cast
        HouSenCurCam_df = housencurcam_typecast.typecast_HouSenCurCam_df(
            HouSenCurCam_df
        )

        # Feature Engineering
        HouSenCurCam_df = housencurcam_features.features_HouSenCurCam_df(
            HouSenCurCam_df
        )

        # Save Data in processed Layer
        write_df_to_parquet(HouSenCurCam_df, "HouSenCurCam_df", silver_path)

        logger.info("HouSenCurCam_df processing completed successfully.")

    except Exception as e:
        logger.error(
            f"An error occurred during HouSenCurCam_df processing: {e}", exc_info=True
        )
        raise


def OpEx_df_processing(OpEx_df, silver_path):
    try:
        # Clean data
        OpEx_df = base_clean.base_clean(OpEx_df)
        OpEx_df = opex_clean.clean_OpEx_df(OpEx_df)

        # Type cast
        OpEx_df = opex_typecast.typecast_OpEx_df(OpEx_df)

        # Feature Engineering
        OpEx_df = opex_features.features_OpEx_df(OpEx_df)

        # Save Data in processed Layer
        write_df_to_parquet(OpEx_df, "OpEx_df", silver_path)

        logger.info("OpEx_df processing completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during OpEx_df processing: {e}", exc_info=True)
        raise


def PacSum_df_processing(PacSum_df, silver_path):
    try:
        # Clean data
        PacSum_df = base_clean.base_clean(PacSum_df)
        PacSum_df = pacsum_clean.clean_PacSum_df(PacSum_df)

        # Type cast
        PacSum_df = pacsum_typecast.typecast_PacSum_df(PacSum_df)

        # Feature Engineering
        PacSum_df = pacsum_features.features_PacSum_df(PacSum_df)

        # Save Data in processed Layer
        write_df_to_parquet(PacSum_df, "PacSum_df", silver_path)

        logger.info("PacSum_df processing completed successfully.")

    except Exception as e:
        logger.error(
            f"An error occurred during PacSum_df processing: {e}", exc_info=True
        )
        raise


def CandMast_df_processing(CandMast_df, silver_path):
    try:
        # Clean data
        CandMast_df = base_clean.base_clean(CandMast_df)
        CandMast_df = candmast_clean.clean_CandMast_df(CandMast_df)

        # Type cast
        CandMast_df = candmast_typecast.typecast_CandMast_df(CandMast_df)

        # Feature Engineering
        CandMast_df = candmast_features.features_CandMast_df(CandMast_df)

        # Save Data in processed Layer
        write_df_to_parquet(CandMast_df, "CandMast_df", silver_path)

        logger.info("CandMast_df processing completed successfully.")
    except Exception as e:
        logger.error(
            f"An error occurred during CandMast_df processing: {e}", exc_info=True
        )
        raise


def CommMast_df_processing(CommMast_df, silver_path):
    try:
        # Clean data
        CommMast_df = base_clean.base_clean(CommMast_df)
        CommMast_df = commmast_clean.clean_CommMast_df(CommMast_df)

        # Feature Engineering
        CommMast_df = commmast_features.features_CommMast_df(CommMast_df)

        # Save Data in processed Layer
        write_df_to_parquet(CommMast_df, "CommMast_df", silver_path)

        logger.info("CommMast_df processing completed successfully.")

    except Exception as e:
        logger.error(
            f"An error occurred during CommMast_df processing: {e}", exc_info=True
        )
        raise


def run_processing_pipeline():
    """
    Main function to run the processing pipeline.
    """
    logger.info("Starting processing pipeline...")
    try:
        # Load configuration and create Spark session
        config_yml = load_config("config/pipeline_config.yaml")
        spark = spark_session(config_yml, app_name="DataProcessing")

        # Setting read data directory path
        bronze_path = config_yml["bronze_path"]
        # Setting processed data directory path
        silver_path = config_yml["silver_path"]

        # Read  from parquet

        # All candidates Data
        AllCand_df = spark.read.parquet(bronze_path + "AllCand_df")

        # Any transaction from one committee to another Data
        TranOneComToAno_df = spark.read.parquet(bronze_path + "TranOneComToAno_df")

        # Candidate-committee linkages
        CanComLink_df = spark.read.parquet(bronze_path + "CanComLink_df")

        # Contributions by individuals
        ConByInd_df = spark.read.parquet(bronze_path + "ConByInd_df")

        # Contributions from committees to candidates & independent expenditure
        ConFromComToCanIndExpen_df = spark.read.parquet(
            bronze_path + "ConFromComToCanIndExpen_df"
        )

        # House Senate current campaigns
        HouSenCurCam_df = spark.read.parquet(bronze_path + "HouSenCurCam_df")

        # Operating expenditures
        OpEx_df = spark.read.parquet(bronze_path + "OpEx_df")

        # PAC summary
        PacSum_df = spark.read.parquet(bronze_path + "PacSum_df")

        # Candidate master
        CandMast_df = spark.read.parquet(bronze_path + "CandMast_df")

        # Commitee master
        CommMast_df = spark.read.parquet(bronze_path + "CommMast_df")

        # Process each DataFrame
        AllCand_df_processing(AllCand_df, silver_path)
        TranOneComToAno_df_processing(TranOneComToAno_df, silver_path)
        CanComLink_df_processing(CanComLink_df, silver_path)
        ConByInd_df_processing(ConByInd_df, silver_path)
        ConFromComToCanIndExpen_df_processing(
            ConFromComToCanIndExpen_df, silver_path
        )
        HouSenCurCam_df_processing(HouSenCurCam_df, silver_path)
        OpEx_df_processing(OpEx_df, silver_path)
        PacSum_df_processing(PacSum_df, silver_path)
        CandMast_df_processing(CandMast_df, silver_path)
        CommMast_df_processing(CommMast_df, silver_path)

        logger.info("Ingestion pipeline completed successfully.")
    except Exception as e:
        logger.error(
            f"An error occurred during the ingestion pipeline: {e}", exc_info=True
        )
        raise


if __name__ == "__main__":
    try:
        run_processing_pipeline()
        logger.info("Processing pipeline finished successfully.")
    except Exception as e:
        logger.error(f"Processing pipeline failed: {e}", exc_info=True)
