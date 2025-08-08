from src.utils.logger import setup_logger

from src.ingestion_exploration.ingestion_run import run_ingestion_pipeline
from src.processing.processing_run import run_processing_pipeline
from src.aggregation.aggregation_run import run_aggregation_pipeline
from src.visualization.visualization_run import run_visualization

logger = setup_logger("pipeline_run", "logs/pipeline_run.log")

def run_full_pipeline():
    try:
        logger.info("Starting full FEC campaign finance pipeline...")

        # Step 1: Ingestion & Exploration
        logger.info("Running ingestion phase...")
        run_ingestion_pipeline()

        # Step 2: Processing
        logger.info("Running processing phase...")
        run_processing_pipeline()

        # Step 3: Aggregation
        logger.info("Running aggregation phase...")
        run_aggregation_pipeline()

        # Step 4: Visualization
        logger.info("Running visualization phase...")
        run_visualization()

        logger.info("Full pipeline executed successfully!")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise e


if __name__ == "__main__":
    run_full_pipeline()
