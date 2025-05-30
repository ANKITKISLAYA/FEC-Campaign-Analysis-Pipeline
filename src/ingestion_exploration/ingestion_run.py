from src.utils.load_config import load_config
from src.utils.spark_session import spark_session
from src.utils.logger import setup_logger

from src.ingestion_exploration.explore import check_data_shape
from src.ingestion_exploration.gcs_ingestion import read_data
from src.ingestion_exploration.save_data import save_to_parquet
from src.ingestion_exploration.mysql_ingestion import read_table_to_df

logger = setup_logger("ingestion_run", "logs/ingestion.log")


def AllCand_df_pipeline(spark, raw_dir_path, parq_dir_path):
    """
    Pipeline function to process All Candidates data.

    """
    try:
        # Read Data
        AllCand_path = raw_dir_path + "All candidates/weball20.txt"
        AllCand_columns = [
            "CAND_ID",
            "CAND_NAME",
            "CAND_ICI",
            "PTY_CD",
            "CAND_PTY_AFFILIATION",
            "TTL_RECEIPTS",
            "TRANS_FROM_AUTH",
            "TTL_DISB",
            "TRANS_TO_AUTH",
            "COH_BOP",
            "COH_COP",
            "CAND_CONTRIB",
            "CAND_LOANS",
            "OTHER_LOANS",
            "CAND_LOAN_REPAY",
            "OTHER_LOAN_REPAY",
            "DEBTS_OWED_BY",
            "TTL_INDIV_CONTRIB",
            "CAND_OFFICE_ST",
            "CAND_OFFICE_DISTRICT",
            "SPEC_ELECTION",
            "PRIM_ELECTION",
            "RUN_ELECTION",
            "GEN_ELECTION",
            "GEN_ELECTION_PRECENT",
            "OTHER_POL_CMTE_CONTRIB",
            "POL_PTY_CONTRIB",
            "CVG_END_DT",
            "INDIV_REFUNDS",
            "CMTE_REFUNDS",
        ]

        AllCand_df = read_data(
            spark=spark, file_path=AllCand_path, columns=AllCand_columns
        )

        # Save Data
        save_to_parquet(AllCand_df, parq_dir_path, "AllCand_df")

        # Read Data from Parquet
        AllCand_df = spark.read.parquet(parq_dir_path + "AllCand_df")
        # Check Data Shape
        check_data_shape(AllCand_df, 30, 3980)
        logger.info("AllCand_df pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Error processing AllCand_df: {e}")
        raise


def TranOneComToAno_df_pipeline(spark, raw_dir_path, parq_dir_path):
    """
    Pipeline function to process Transactions from One Committee to Another data.
    """
    try:
        # Read Data
        TranOneComToAno_data = (
            raw_dir_path + "Any transaction from one committee to another/itoth.txt"
        )
        TranOneComToAno_header = (
            raw_dir_path
            + "Any transaction from one committee to another/oth_header_file.csv"
        )

        TranOneComToAno_df = read_data(
            spark=spark, file_path=TranOneComToAno_data, header=TranOneComToAno_header
        )

        # Save Data
        save_to_parquet(TranOneComToAno_df, parq_dir_path, "TranOneComToAno_df")

        # Read Data from Parquet
        TranOneComToAno_df = spark.read.parquet(parq_dir_path + "TranOneComToAno_df")

        # Check Data Shape
        check_data_shape(TranOneComToAno_df, 21, 7401653)
        logger.info("TranOneComToAno_df pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error processing TranOneComToAno_df: {e}")
        raise


def CanComLink_df_pipeline(spark, raw_dir_path, parq_dir_path):
    """
    Pipeline function to process Candidate Committee Link data.
    """
    try:
        # Read Data
        CanComLink_data = raw_dir_path + "Candidate-committee linkages/ccl.txt"
        CanComLink_header = (
            raw_dir_path + "Candidate-committee linkages/ccl_header_file.csv"
        )

        CanComLink_df = read_data(
            spark=spark, file_path=CanComLink_data, header=CanComLink_header
        )

        # Save Data
        save_to_parquet(CanComLink_df, parq_dir_path, "CanComLink_df")

        # Read Data from Parquet
        CanComLink_df = spark.read.parquet(parq_dir_path + "CanComLink_df")

        # Check Data Shape
        check_data_shape(CanComLink_df, 7, 7055)
        logger.info("CanComLink_df pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error processing CanComLink_df: {e}")
        raise


def ConByInd_df_pipeline(spark, raw_dir_path, parq_dir_path):
    """
    Pipeline function to process Contributions by Individuals data.
    """
    try:
        # Read Data
        ConByInd_data = raw_dir_path + "Contributions by individuals/itcont.txt"
        ConByInd_header = (
            raw_dir_path + "Contributions by individuals/indiv_header_file.csv"
        )

        ConByInd_df = read_data(
            spark=spark, file_path=ConByInd_data, header=ConByInd_header
        )

        # Save Data
        save_to_parquet(ConByInd_df, parq_dir_path, "ConByInd_df")

        # Read Data from Parquet
        ConByInd_df = spark.read.parquet(parq_dir_path + "ConByInd_df")

        # Check Data Shape
        check_data_shape(ConByInd_df, 21, 69377425)
        logger.info("ConByInd_df pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error processing ConByInd_df: {e}")
        raise


def ConFromComToCanIndExpen_df_pipeline(spark, raw_dir_path, parq_dir_path):
    """
    Pipeline function to process Contributions from Committees to Candidates and Independent Expenditures data.
    """
    try:
        # Read Data
        ConFromComToCanIndExpen_data = (
            raw_dir_path
            + "Contributions from committees to candidates & independent expenditure/itpas2.txt"
        )
        ConFromComToCanIndExpen_header = (
            raw_dir_path
            + "Contributions from committees to candidates & independent expenditure/pas2_header_file.csv"
        )

        ConFromComToCanIndExpen_df = read_data(
            spark=spark,
            file_path=ConFromComToCanIndExpen_data,
            header=ConFromComToCanIndExpen_header,
        )

        # Save Data
        save_to_parquet(
            ConFromComToCanIndExpen_df, parq_dir_path, "ConFromComToCanIndExpen_df"
        )

        # Read Data from Parquet
        ConFromComToCanIndExpen_df = spark.read.parquet(
            parq_dir_path + "ConFromComToCanIndExpen_df"
        )

        # Check Data Shape
        check_data_shape(ConFromComToCanIndExpen_df, 22, 887829)
        logger.info("ConFromComToCanIndExpen_df pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error processing ConFromComToCanIndExpen_df: {e}")
        raise


def HouSenCurCam_df_pipeline(spark, raw_dir_path, parq_dir_path):
    """
    Pipeline function to process House and Senate Current Campaign data.
    """
    try:
        # Read Data
        HouSenCurCam_data = raw_dir_path + "House Senate current campaigns/webl20.txt"
        HouSenCurCam_columns = [
            "CAND_ID",
            "CAND_NAME",
            "CAND_ICI",
            "PTY_CD",
            "CAND_PTY_AFFILIATION",
            "TTL_RECEIPTS",
            "TRANS_FROM_AUTH",
            "TTL_DISB",
            "TRANS_TO_AUTH",
            "COH_BOP",
            "COH_COP",
            "CAND_CONTRIB",
            "CAND_LOANS",
            "OTHER_LOANS",
            "CAND_LOAN_REPAY",
            "OTHER_LOAN_REPAY",
            "DEBTS_OWED_BY",
            "TTL_INDIV_CONTRIB",
            "CAND_OFFICE_ST",
            "CAND_OFFICE_DISTRICT",
            "SPEC_ELECTION",
            "PRIM_ELECTION",
            "RUN_ELECTION",
            "GEN_ELECTION",
            "GEN_ELECTION_PRECENT",
            "OTHER_POL_CMTE_CONTRIB",
            "POL_PTY_CONTRIB",
            "CVG_END_DT",
            "INDIV_REFUNDS",
            "CMTE_REFUNDS",
        ]

        HouSenCurCam_df = read_data(
            spark=spark, file_path=HouSenCurCam_data, columns=HouSenCurCam_columns
        )

        # Save Data
        save_to_parquet(HouSenCurCam_df, parq_dir_path, "HouSenCurCam_df")

        # Read Data from Parquet
        HouSenCurCam_df = spark.read.parquet(parq_dir_path + "HouSenCurCam_df")

        # Check Data Shape
        check_data_shape(HouSenCurCam_df, 30, 2638)
        logger.info("HouSenCurCam_df pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error processing HouSenCurCam_df: {e}")
        raise


def OpEx_df_pipeline(spark, raw_dir_path, parq_dir_path):
    """
    Pipeline function to process Operating Expenditures data.
    """
    try:
        # Read Data
        OpEx_data = raw_dir_path + "Operating expenditures/oppexp.txt"

        OpEx_columns = [
            "CMTE_ID",
            "AMNDT_IND",
            "RPT_YR",
            "RPT_TP",
            "IMAGE_NUM",
            "LINE_NUM",
            "FORM_TP_CD",
            "SCHED_TP_CD",
            "NAME",
            "CITY",
            "STATE",
            "ZIP_CODE",
            "TRANSACTION_DT",
            "TRANSACTION_AMT",
            "TRANSACTION_PGI",
            "PURPOSE",
            "CATEGORY",
            "CATEGORY_DESC",
            "MEMO_CD",
            "MEMO_TEXT",
            "ENTITY_TP",
            "SUB_ID",
            "FILE_NUM",
            "TRAN_ID",
            "BACK_REF_TRAN_ID",
            "extra_column",  # added missing column
        ]

        OpEx_df = read_data(spark=spark, file_path=OpEx_data, columns=OpEx_columns)

        # Save Data
        save_to_parquet(OpEx_df, parq_dir_path, "OpEx_df")

        # Read Data from Parquet
        OpEx_df = spark.read.parquet(parq_dir_path + "OpEx_df")

        # Check Data Shape
        check_data_shape(OpEx_df, 26, 2310524)
        logger.info("OpEx_df pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error processing OpEx_df: {e}")
        raise


def PacSum_df_pipeline(spark, raw_dir_path, parq_dir_path):
    """
    Pipeline function to process PAC Summary data.
    """
    try:
        # Read Data

        PacSum_data = raw_dir_path + "PAC summary/webk20.txt"

        PacSum_columns = [
            "CMTE_ID",
            "CMTE_NM",
            "CMTE_TP",
            "CMTE_DSGN",
            "CMTE_FILING_FREQ",
            "TTL_RECEIPTS",
            "TRANS_FROM_AFF",
            "INDV_CONTRIB",
            "OTHER_POL_CMTE_CONTRIB",
            "CAND_CONTRIB",
            "CAND_LOANS",
            "TTL_LOANS_RECEIVED",
            "TTL_DISB",
            "TRANF_TO_AFF",
            "INDV_REFUNDS",
            "OTHER_POL_CMTE_REFUNDS",
            "CAND_LOAN_REPAY",
            "LOAN_REPAY",
            "COH_BOP",
            "COH_COP",
            "DEBTS_OWED_BY",
            "NONFED_TRANS_RECEIVED",
            "CONTRIB_TO_OTHER_CMTE",
            "IND_EXP",
            "PTY_COORD_EXP",
            "NONFED_SHARE_EXP",
            "CVG_END_DT",
        ]

        PacSum_df = read_data(
            spark=spark, file_path=PacSum_data, columns=PacSum_columns
        )
        logger.info("PAC Summary data read successfully.")
    except Exception as e:
        logger.error(f"Error reading PAC Summary data: {e}")
        raise

    # Save Data
    save_to_parquet(PacSum_df, parq_dir_path, "PacSum_df")

    # Read Data from Parquet
    PacSum_df = spark.read.parquet(parq_dir_path + "PacSum_df")

    # Check Data Shape
    check_data_shape(PacSum_df, 27, 11539)


def CandMast_df_pipeline(spark, config_yml, parq_dir_path):
    """
    Pipeline function to process Candidate Master data.
    """
    try:
        # Read table into DataFrame
        df_Candidate_master = read_table_to_df("Candidate_master", config_yml)

        # Convert pandas df to spark df
        CandMast_df = spark.createDataFrame(df_Candidate_master)

        # Save Data
        save_to_parquet(CandMast_df, parq_dir_path, "CandMast_df")

        # Read Data from parquet
        CandMast_df = spark.read.parquet(parq_dir_path + "CandMast_df")

        # Check Data Shape
        check_data_shape(CandMast_df, 15, 7758)
        logger.info("CandMast_df pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error processing CandMast_df: {e}")
        raise


def CommMast_df_pipeline(spark, config_yml, parq_dir_path):
    """
    Pipeline function to process Committee Master data.
    """
    try:
        # Read table into DataFrame
        df_Committee_master = read_table_to_df("Committee_master", config_yml)

        # Convert pandas df to spark df
        CommMast_df = spark.createDataFrame(df_Committee_master)

        # Save Data
        save_to_parquet(CommMast_df, parq_dir_path, "CommMast_df")

        # Read Data from parquet
        CommMast_df = spark.read.parquet(parq_dir_path + "CommMast_df")

        # Check Data Shape
        check_data_shape(CommMast_df, 15, 18286)
        logger.info("CommMast_df pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error processing CommMast_df: {e}")
        raise


def run_ingestion_pipeline():
    """
    Main function to run the ingestion process.
    """
    logger.info("Starting ingestion pipeline...")
    try:
        # Load configuration and create Spark session
        config_yml = load_config("config/pipeline_config.yaml")
        spark = spark_session(config_yml, app_name="DataIngestionExploration")

        # Setting raw data directory path
        raw_dir_path = config_yml["raw_data_path"]
        # Setting write data directory path
        parq_dir_path = config_yml["parquet_data_path"]

        # Run individual data pipelines
        AllCand_df_pipeline(spark, raw_dir_path, parq_dir_path)
        TranOneComToAno_df_pipeline(spark, raw_dir_path, parq_dir_path)
        CanComLink_df_pipeline(spark, raw_dir_path, parq_dir_path)
        ConByInd_df_pipeline(spark, raw_dir_path, parq_dir_path)
        ConFromComToCanIndExpen_df_pipeline(spark, raw_dir_path, parq_dir_path)
        HouSenCurCam_df_pipeline(spark, raw_dir_path, parq_dir_path)
        OpEx_df_pipeline(spark, raw_dir_path, parq_dir_path)
        PacSum_df_pipeline(spark, raw_dir_path, parq_dir_path)
        CandMast_df_pipeline(spark, config_yml, parq_dir_path)
        CommMast_df_pipeline(spark, config_yml, parq_dir_path)

        logger.info("Ingestion pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Error in ingestion pipeline: {e}")
        raise
    finally:
        try:
            # Stop the Spark session
            spark.stop()
            logger.info("Spark session stopped successfully.")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {e}")
            raise


if __name__ == "__main__":
    try:
        run_ingestion_pipeline()
        logger.info("Ingestion pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Error executing ingestion pipeline: {e}")
        raise
