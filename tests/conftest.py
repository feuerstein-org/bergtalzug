"""Test fixtures for ETL Pipeline tests."""

from dataclasses import dataclass
import pytest
from typing import Any
from bergtalzug import ETLPipeline, ETLPipelineConfig, WorkItem
from pytest_mock import MockerFixture
import asyncio
import time


def work_item_factory(count: int = 1, data: Any = "test_data") -> list[WorkItem]:
    """Create a list of WorkItem instances with custom data."""
    work_items: list[WorkItem] = []
    for _ in range(count):
        work_items.append(WorkItem(data=data))
    return work_items


@dataclass
class MockETLPipelineConfig:
    """
    Configuration for the MockETLPipeline class.

    No validation is performed here. This class is used to have lower testing defaults
    and is passed to the ETLPipelineConfig class which performs validation.
    """

    pipeline_name: str = "mock_etl_pipeline"
    fetch_workers: int = 1
    process_workers: int = 1
    store_workers: int = 1
    fetch_queue_size: int = 10
    process_queue_size: int = 5
    store_queue_size: int = 10
    queue_refresh_rate: float = 0.01  # seconds
    stats_interval_seconds: float = 1.0
    enable_tracking: bool = True
    # Test specific parameters
    items_to_process: list[WorkItem] | None = None
    process_sleep: float | None = None
    process_is_sync: bool = False  # Whether to use sync_process or async process


class MockETLPipeline(ETLPipeline):
    """
    Mock implementation of ETLPipeline for testing purposes.

    This class overrides the abstract methods to provide mock behavior.
    You can call mock methods on mock_refill_queue, mock_fetch, mock_process, mock_store

    Each worker pool can have the size set to 0 to disable it for testing.
    """

    def __init__(self, mocker: MockerFixture, config: MockETLPipelineConfig) -> None:
        """Initialize the mock ETL pipeline with configuration."""
        # Use default config if none provided

        # Pass ETL pipeline parameters to parent constructor
        etl_config = ETLPipelineConfig(
            pipeline_name=config.pipeline_name,
            fetch_workers=config.fetch_workers,
            process_workers=config.process_workers,
            store_workers=config.store_workers,
            fetch_queue_size=config.fetch_queue_size,
            process_queue_size=config.process_queue_size,
            store_queue_size=config.store_queue_size,
            queue_refresh_rate=config.queue_refresh_rate,
            stats_interval_seconds=config.stats_interval_seconds,
            enable_tracking=config.enable_tracking,
        )

        self.mock_process_is_sync = config.process_is_sync
        super().__init__(config=etl_config)

        # Store configuration
        self.config = config

        # TODO: Should be replaced soon once we allow WorkItems to be set on startup
        self.items_to_process: list[WorkItem] = config.items_to_process or []

        # Create mocks with implementation
        self.mock_refill_queue = mocker.AsyncMock(side_effect=self._refill_queue_impl)
        self.mock_fetch = mocker.AsyncMock(side_effect=self._fetch_impl)
        self.mock_process = mocker.Mock(side_effect=self._process_impl)
        self.mock_sync_process = mocker.Mock(side_effect=self._sync_process_impl)
        self.mock_store = mocker.AsyncMock(side_effect=self._store_impl)

        # Replace the actual methods with mocks
        self.refill_queue = self.mock_refill_queue
        self.fetch = self.mock_fetch
        self.process = self.mock_process
        self.sync_process = self.mock_sync_process
        self.store = self.mock_store

    # Bypass sync validation for testing
    def _check_process_is_sync(self) -> bool:
        """Override validation for testing purposes."""
        return self.mock_process_is_sync

    async def _refill_queue_impl(self, count: int) -> list[WorkItem]:
        if self.items_to_process:
            batch = self.items_to_process[:count]
            self.items_to_process = self.items_to_process[count:]
            return batch
        return []

    async def _fetch_impl(self, item: WorkItem) -> WorkItem:
        return item

    async def _process_impl(self, item: WorkItem) -> WorkItem:
        if self.config.process_sleep is not None and self.config.process_sleep > 0:
            await asyncio.sleep(self.config.process_sleep)
        item.data = "processed_" + item.data
        return item

    def _sync_process_impl(self, item: WorkItem) -> WorkItem:
        if self.config.process_sleep is not None and self.config.process_sleep > 0:
            time.sleep(self.config.process_sleep)
        item.data = "processed_" + item.data
        return item

    async def _store_impl(self, item: WorkItem) -> None:
        return


class MockETLPipelineFactory:
    """Collection of factory methods to create MockETLPipeline instances."""

    def __init__(self, mocker: MockerFixture) -> None:
        """Initialize the mocker."""
        self._mocker = mocker

    def create(
        self, config: MockETLPipelineConfig | None = None, work_items_count: int | None = None, **kwargs: Any
    ) -> MockETLPipeline:
        """
        Create a MockETLPipeline with the given configuration.

        Will always overwrite items_to_process in config if work_items_count is passed.
        Will ignore kwargs if config is passed.
        """
        if config is None:
            config = MockETLPipelineConfig(**kwargs)
        if work_items_count:
            work_items = work_item_factory(count=work_items_count)
            config.items_to_process = work_items
        return MockETLPipeline(self._mocker, config)

    def create_sync(
        self, config: MockETLPipelineConfig | None = None, work_items_count: int | None = None, **kwargs: Any
    ) -> MockETLPipeline:
        """
        Create a MockETLPipeline with the given configuration and use threads for processing.

        Will always overwrite items_to_process in config if work_items_count is passed.
        Will ignore kwargs if config is passed.
        """
        if config is None:
            config = MockETLPipelineConfig(**kwargs)
        config.process_is_sync = True
        if work_items_count:
            work_items = work_item_factory(count=work_items_count)
            config.items_to_process = work_items
        return MockETLPipeline(self._mocker, config)


@pytest.fixture
def mock_etl_pipeline_factory(mocker: MockerFixture) -> MockETLPipelineFactory:
    """Fixture providing a MockETLPipelineFactory for creating test pipelines"""
    # This is needed to disable the validation of existence of
    # abstract methods since we are mocking them
    MockETLPipeline.__abstractmethods__ = frozenset()

    return MockETLPipelineFactory(mocker)
