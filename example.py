"""Sample ETL pipeline implementation"""

from bergtalzug import ETLPipeline, WorkItem, ETLPipelineConfig
import asyncio
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class CustomETLPipeline(ETLPipeline):
    """Custom ETL Pipeline Implementation"""

    def __init__(self, config: ETLPipelineConfig | None = None) -> None:
        """Init function"""
        super().__init__(config)
        self.logger = logging.getLogger(f"etl.{config.pipeline_name if config else 'pipeline'}")
        # Sample limit to stop after certain items
        self.counter = 0
        self.limit = 20

    async def refill_queue(self, count: int) -> list[WorkItem]:
        """Sample refill_queue method."""
        self.logger.info("Refilling data...")
        if self.counter <= self.limit:
            if self.counter + count > self.limit:
                work_items = [WorkItem(data=b"test") for _ in range(self.limit - self.counter)]
                self.counter += count
                return work_items
            self.counter += count
            return [WorkItem(data=b"test") for _ in range(count)]
        return []

    async def fetch(self, item: WorkItem) -> WorkItem:
        """Sample fetch method."""
        self.logger.info("Fetching data...")
        await asyncio.sleep(0.5)
        return item

    async def process(self, item: WorkItem) -> WorkItem:
        """Sample process method."""
        self.logger.info("Processing data...")
        await asyncio.sleep(0.5)

        return item

    async def store(self, item: WorkItem) -> None:
        """Sample store method."""
        self.logger.info("Storing data...")
        await asyncio.sleep(0.5)


async def main() -> None:
    """Main function to run the ETL pipeline."""
    config = ETLPipelineConfig(
        pipeline_name="test-pipeline",
        fetch_workers=1,
        process_workers=1,
        store_workers=1,
        fetch_queue_size=10,
        process_queue_size=1,
        store_queue_size=1,
        queue_refresh_rate=0.1,
        stats_interval_seconds=3,
    )
    my_pipeline = CustomETLPipeline(config)

    await my_pipeline.run()


asyncio.run(main())
