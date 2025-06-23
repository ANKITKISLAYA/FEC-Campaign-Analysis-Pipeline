import pytest
from pyspark.sql import SparkSession

# Bronze data path
bronze_path = "gs://dataproc-staging-us-central1-40371648517-ndvgfbwp/notebooks/jupyter/FEC-Campaign-Analysis/FEC-Data/bronze/"

# Expected column schemas
EXPECTED_SCHEMAS = {
    "AllCand_df": [
        "CAND_ID", "CAND_NAME", "CAND_ICI", "PTY_CD", "CAND_PTY_AFFILIATION", "TTL_RECEIPTS",
        "TRANS_FROM_AUTH", "TTL_DISB", "TRANS_TO_AUTH", "COH_BOP", "COH_COP", "CAND_CONTRIB",
        "CAND_LOANS", "OTHER_LOANS", "CAND_LOAN_REPAY", "OTHER_LOAN_REPAY", "DEBTS_OWED_BY",
        "TTL_INDIV_CONTRIB", "CAND_OFFICE_ST", "CAND_OFFICE_DISTRICT", "SPEC_ELECTION",
        "PRIM_ELECTION", "RUN_ELECTION", "GEN_ELECTION", "GEN_ELECTION_PRECENT",
        "OTHER_POL_CMTE_CONTRIB", "POL_PTY_CONTRIB", "CVG_END_DT", "INDIV_REFUNDS", "CMTE_REFUNDS"
    ],
    "CanComLink_df": [
        "CAND_ID", "CAND_ELECTION_YR", "FEC_ELECTION_YR", "CMTE_ID", "CMTE_TP",
        "CMTE_DSGN", "LINKAGE_ID"
    ],
    "CandMast_df": [
        "CAND_ID", "CAND_NAME", "CAND_PTY_AFFILIATION", "CAND_ELECTION_YR", "CAND_OFFICE_ST",
        "CAND_OFFICE", "CAND_OFFICE_DISTRICT", "CAND_ICI", "CAND_STATUS", "CAND_PCC",
        "CAND_ST1", "CAND_ST2", "CAND_CITY", "CAND_ST", "CAND_ZIP"
    ],
    "CommMast_df": [
        "CMTE_ID", "CMTE_NM", "TRES_NM", "CMTE_ST1", "CMTE_ST2", "CMTE_CITY", "CMTE_ST",
        "CMTE_ZIP", "CMTE_DSGN", "CMTE_TP", "CMTE_PTY_AFFILIATION", "CMTE_FILING_FREQ",
        "ORG_TP", "CONNECTED_ORG_NM", "CAND_ID"
    ],
    "ConByInd_df": [
        "CMTE_ID", "AMNDT_IND", "RPT_TP", "TRANSACTION_PGI", "IMAGE_NUM", "TRANSACTION_TP",
        "ENTITY_TP", "NAME", "CITY", "STATE", "ZIP_CODE", "EMPLOYER", "OCCUPATION",
        "TRANSACTION_DT", "TRANSACTION_AMT", "OTHER_ID", "TRAN_ID", "FILE_NUM", "MEMO_CD",
        "MEMO_TEXT", "SUB_ID"
    ],
    "ConFromComToCanIndExpen_df": [
        "CMTE_ID", "AMNDT_IND", "RPT_TP", "TRANSACTION_PGI", "IMAGE_NUM", "TRANSACTION_TP",
        "ENTITY_TP", "NAME", "CITY", "STATE", "ZIP_CODE", "EMPLOYER", "OCCUPATION",
        "TRANSACTION_DT", "TRANSACTION_AMT", "OTHER_ID", "CAND_ID", "TRAN_ID",
        "FILE_NUM", "MEMO_CD", "MEMO_TEXT", "SUB_ID"
    ],
    "HouSenCurCam_df": [
        "CAND_ID", "CAND_NAME", "CAND_ICI", "PTY_CD", "CAND_PTY_AFFILIATION", "TTL_RECEIPTS",
        "TRANS_FROM_AUTH", "TTL_DISB", "TRANS_TO_AUTH", "COH_BOP", "COH_COP", "CAND_CONTRIB",
        "CAND_LOANS", "OTHER_LOANS", "CAND_LOAN_REPAY", "OTHER_LOAN_REPAY", "DEBTS_OWED_BY",
        "TTL_INDIV_CONTRIB", "CAND_OFFICE_ST", "CAND_OFFICE_DISTRICT", "SPEC_ELECTION",
        "PRIM_ELECTION", "RUN_ELECTION", "GEN_ELECTION", "GEN_ELECTION_PRECENT",
        "OTHER_POL_CMTE_CONTRIB", "POL_PTY_CONTRIB", "CVG_END_DT", "INDIV_REFUNDS", "CMTE_REFUNDS"
    ],
    "OpEx_df": [
        "CMTE_ID", "AMNDT_IND", "RPT_YR", "RPT_TP", "IMAGE_NUM", "LINE_NUM", "FORM_TP_CD",
        "SCHED_TP_CD", "NAME", "CITY", "STATE", "ZIP_CODE", "TRANSACTION_DT", "TRANSACTION_AMT",
        "TRANSACTION_PGI", "PURPOSE", "CATEGORY", "CATEGORY_DESC", "MEMO_CD", "MEMO_TEXT",
        "ENTITY_TP", "SUB_ID", "FILE_NUM", "TRAN_ID", "BACK_REF_TRAN_ID", "extra_column"
    ],
    "PacSum_df": [
        "CMTE_ID", "CMTE_NM", "CMTE_TP", "CMTE_DSGN", "CMTE_FILING_FREQ", "TTL_RECEIPTS",
        "TRANS_FROM_AFF", "INDV_CONTRIB", "OTHER_POL_CMTE_CONTRIB", "CAND_CONTRIB", "CAND_LOANS",
        "TTL_LOANS_RECEIVED", "TTL_DISB", "TRANF_TO_AFF", "INDV_REFUNDS", "OTHER_POL_CMTE_REFUNDS",
        "CAND_LOAN_REPAY", "LOAN_REPAY", "COH_BOP", "COH_COP", "DEBTS_OWED_BY",
        "NONFED_TRANS_RECEIVED", "CONTRIB_TO_OTHER_CMTE", "IND_EXP", "PTY_COORD_EXP",
        "NONFED_SHARE_EXP", "CVG_END_DT"
    ],
    "TranOneComToAno_df": [
        "CMTE_ID", "AMNDT_IND", "RPT_TP", "TRANSACTION_PGI", "IMAGE_NUM", "TRANSACTION_TP",
        "ENTITY_TP", "NAME", "CITY", "STATE", "ZIP_CODE", "EMPLOYER", "OCCUPATION",
        "TRANSACTION_DT", "TRANSACTION_AMT", "OTHER_ID", "TRAN_ID", "FILE_NUM", "MEMO_CD",
        "MEMO_TEXT", "SUB_ID"
    ]
}

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("TestBronzeSchemas").getOrCreate()
    yield spark
    spark.stop() # after yield, stop the Spark session

@pytest.mark.parametrize("file_name,expected_columns", EXPECTED_SCHEMAS.items())
def test_bronze_schema(spark, file_name, expected_columns):
    path = f"{bronze_path}{file_name}"
    df = spark.read.parquet(path)
    actual_columns = df.columns
    assert set(actual_columns) == set(expected_columns), (
        f"Schema mismatch for {file_name}\nExpected: {expected_columns}\nGot: {actual_columns}")
