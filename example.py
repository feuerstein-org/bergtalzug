"""Sample ETL pipeline implementation using dynamic stages"""

from bergtalzug import (
    ETLPipeline,
    WorkItem,
    ETLPipelineConfig,
    StageConfig,
    ExecutionType,
    WorkItemResult,
)
import asyncio
import logging
from typing import Any
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class CustomETLPipeline(ETLPipeline):
    """Custom ETL Pipeline Implementation"""

    def __init__(self, config: ETLPipelineConfig, total_items: int = 20) -> None:
        """Init function"""
        super().__init__(config)
        self.logger = logging.getLogger(f"etl.{config.pipeline_name}")
        self.counter = 0
        self.total_items = total_items

    async def refill_queue(self, count: int) -> list[WorkItem]:
        """Sample refill_queue method."""
        items: list[WorkItem] = []

        for _ in range(count):
            if self.counter >= self.total_items:
                break

            items.append(WorkItem(data={"item_id": self.counter, "value": f"test_{self.counter}"}))
            self.counter += 1

        if items:
            self.logger.info("Refilled queue with %s items", len(items))

        return items


# Define stage handlers as standalone functions
# Each demonstrates a different execution type


async def fetch_data(work_item: WorkItem) -> WorkItem:
    """Async fetch stage - simulates async data fetching from API/database."""
    logging.info("[ASYNC] Fetching data for item %s...", work_item.job_id)
    await asyncio.sleep(0.3)
    work_item.data["fetched"] = True
    return work_item


def transform_data(work_item: WorkItem) -> Any:
    """Thread-based transform stage - simulates CPU-bound transformation."""
    logging.info("[THREAD] Transforming data for item %s...", work_item.job_id)
    time.sleep(0.3)
    work_item.data["transformed"] = True
    return work_item


def validate_data(job_id: str, data: Any) -> Any:
    """Process-based validate stage - simulates CPU-intensive validation."""
    logging.info("[PROCESS] Validating data for item %s...", job_id)
    time.sleep(0.3)
    data["validated"] = True
    return data


def enrich_data(job_id: str, data: Any) -> Any:
    """Interpreter-based enrich stage - simulates data enrichment."""
    # Note: For InterpreterPoolExecutor, all imports must be done inside the function
    # and cannot use module-level imports as they aren't shareable between interpreters
    # Also, logging is not compatible with sub-interpreters, but print() works fine
    import time

    print(f"[INTERPRETER] Enriching data for item {job_id}...")  # noqa: T201
    time.sleep(0.3)
    data["enriched"] = True
    return data


async def store_data(work_item: WorkItem) -> WorkItem:
    """Async store stage - simulates async data storage to S3/database."""
    logging.info("[ASYNC] Storing data for item %s...", work_item.job_id)
    await asyncio.sleep(0.3)
    work_item.data["stored"] = True
    return work_item


async def main() -> None:
    """Main function to run the ETL pipeline."""
    # Define pipeline configuration with 5 stages using different execution types
    config = ETLPipelineConfig(
        pipeline_name="test-pipeline",
        stages=[
            StageConfig(
                name="fetch",
                execution_type=ExecutionType.ASYNC,  # Async for I/O operations
                workers=3,
                queue_size=10,
            ),
            StageConfig(
                name="transform",
                execution_type=ExecutionType.THREAD,  # Thread pool for CPU-bound work
                workers=2,
                queue_size=5,
            ),
            StageConfig(
                name="validate",
                execution_type=ExecutionType.PROCESS,  # Process pool for CPU-intensive work
                workers=2,
                queue_size=5,
            ),
            StageConfig(
                name="enrich",
                execution_type=ExecutionType.INTERPRETER,  # Interpreter pool (Python 3.14+)
                workers=2,
                queue_size=5,
            ),
            StageConfig(
                name="store",
                execution_type=ExecutionType.ASYNC,  # Async for I/O operations
                workers=3,
                queue_size=10,
            ),
        ],
        queue_refresh_rate=0.1,
        stats_interval_seconds=3.0,
        enable_tracking=True,
    )

    # Create pipeline instance
    my_pipeline = CustomETLPipeline(config, total_items=20)

    # Register handlers for each stage
    my_pipeline.register_stage_handler("fetch", fetch_data)
    my_pipeline.register_stage_handler("transform", transform_data)
    my_pipeline.register_stage_handler("validate", validate_data)
    my_pipeline.register_stage_handler("enrich", enrich_data)
    my_pipeline.register_stage_handler("store", store_data)

    # Optional: Add completion callback
    def on_item_complete(result: WorkItemResult) -> None:
        if result.success:
            logging.info("✓ Item %s completed successfully", result.job_id)
        else:
            logging.error("✗ Item %s failed: %s", result.job_id, result.error)

    my_pipeline.add_completion_callback(on_item_complete)

    # Run the pipeline
    logging.info("Starting ETL pipeline with 5 stages...")
    logging.info("Stages: ASYNC → THREAD → PROCESS → INTERPRETER → ASYNC")
    results = await my_pipeline.run()

    # Print summary
    logging.info("=" * 50)
    logging.info("Pipeline Execution Summary")
    logging.info("=" * 50)
    logging.info("Total items processed: %s", len(results))
    logging.info("Successful: %s", sum([1 for r in results if r.success]))
    logging.info("Failed: %s", sum([1 for r in results if not r.success]))


if __name__ == "__main__":
    asyncio.run(main())
