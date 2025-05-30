# FEC Campaign Analysis Pipeline

A modular, Spark-based data engineering pipeline to analyze campaign finance data from the [Federal Election Commission (FEC)](https://www.fec.gov/data/browse-data/?tab=bulk-data). This project demonstrates end-to-end capabilities in data ingestion, processing, aggregation, and visualization using PySpark, ideal for large-scale financial datasets.

---

## Project Structure

```
FEC-Campaign-Analysis-Pipeline/
├── config/
│   └── pipeline_config.yaml               # Main configuration file

├── logs/                                  # Logs generated on pipeline run
│   ├── aggregation.log
│   ├── ingestion.log
│   ├── pipeline_run.log
│   ├── processing.log
│   └── visualization.log

├── notebooks/                             # Jupyter notebooks for EDA and dev
│   ├── 1.DataIngestionAndExploration.ipynb
│   ├── 2.DataCleaningAndTransformation.ipynb
│   ├── 3.DataIntegrationAndAggregation.ipynb
│   └── 4.DataVisualization.ipynb

├── src/
│   ├── pipeline_run.py                    # Master pipeline runner (all-in-one)

│   ├── ingestion_exploration/             # Ingestion-related modules
│   │   ├── explore.py
│   │   ├── gcs_ingestion.py
│   │   ├── ingestion_run.py
│   │   ├── mysql_ingestion.py
│   │   └── save_data.py

│   ├── processing/                        # Processing modules
│   │   ├── cleaning/
│   │   │   ├── base_clean.py
│   │   │   ├── allcand_clean.py
│   │   │   ├── tranonecomtoano_clean.py
│   │   │   ├── cancomlink_clean.py
│   │   │   ├── conbyind_clean.py
│   │   │   ├── confromcomtotoanind_clean.py
│   │   │   ├── housencurcam_clean.py
│   │   │   ├── opex_clean.py
│   │   │   ├── pacsum_clean.py
│   │   │   ├── candmast_clean.py
│   │   │   └── commmast_clean.py
│   │
│   │   ├── typecasting/
│   │   │   ├── allcand_typecast.py
│   │   │   ├── tranonecomtoano_typecast.py
│   │   │   ├── cancomlink_typecast.py
│   │   │   ├── conbyind_typecast.py
│   │   │   ├── confromcomtotoanind_typecast.py
│   │   │   ├── housencurcam_typecast.py
│   │   │   ├── opex_typecast.py
│   │   │   ├── pacsum_typecast.py
│   │   │   ├── candmast_typecast.py
│   │   │   └── commmast_typecast.py
│   │
│   │   ├── features/
│   │   │   ├── allcand_features.py
│   │   │   ├── tranonecomtoano_features.py
│   │   │   ├── cancomlink_features.py
│   │   │   ├── conbyind_features.py
│   │   │   ├── confromcomtotoanind_features.py
│   │   │   ├── housencurcam_features.py
│   │   │   ├── opex_features.py
│   │   │   ├── pacsum_features.py
│   │   │   ├── candmast_features.py
│   │   │   └── commmast_features.py
│   │
│   │   ├── utils/
│   │   │   ├── null_counts.py
│   │   │   └── write_parquet.py
│   │
│   │   └── processing_run.py              # Main runner for processing pipeline

│   ├── aggregation/                       # Data integration & aggregation
│   │   ├── aggregate_donor.py
│   │   ├── aggregation_run.py
│   │   ├── donor_pattern.py
│   │   ├── individual_contribution.py
│   │   ├── save_data.py
│   │   └── spending_pattern.py

│   ├── visualization/                     # All visual analytics code
│   │   ├── plots/                         # Folder where plots are saved
│   │   ├── donationanalysis_plot.py
│   │   ├── donorsanalysis_plot.py
│   │   ├── individualcontributions_plot.py
│   │   ├── totalexpenbymonth_plot.py
│   │   └── visualization_run.py

│   └── utils/                             # Reusable utility functions
│       ├── load_config.py
│       ├── logger.py
│       └── spark_session.py

└── README.md                              # Documentation on setup and usage

```

---

## Technologies Used

- **Apache Spark** (PySpark)
- **Google Cloud Storage (GCS)** & **MySQL**
- **Matplotlib & Seaborn** for visualizations
- **Modular Python Packages** for scalability
- **YAML** for dynamic pipeline configuration
- **Logging** for debuggable pipeline stages

---

## Pipeline Stages

| Stage            | Description |
|------------------|-------------|
| Ingestion      | Ingests raw datasets from GCS & MySQL |
| Processing     | Cleans, typecasts, engineers features |
| Aggregation    | Aggregates donor & spending patterns |
| Visualization  | Creates insightful financial plots |
| Integration    | Controlled by `pipeline_run.py` |

---

## How to Run

1. **Install dependencies**  
   ```bash
   pip install -r requirements.txt
2. **Run pipeline_run.py**  
