"""Test fixtures for ETL Pipeline tests."""

from dataclasses import dataclass
import pytest
from typing import Any
from bergtalzug import ETLPipeline, ETLPipelineConfig, WorkItem, StageConfig, ExecutionType
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
    stages: list[StageConfig] | None = None  # If None, creates default 5-stage pipeline
    queue_refresh_rate: float = 0.05  # seconds
    stats_interval_seconds: float = 1.0
    enable_tracking: bool = True
    # Test specific parameters
    items_to_process: list[WorkItem] | None = None
    process_sleep: float | None = None


def create_default_stages() -> list[StageConfig]:
    """Create default 5-stage pipeline for testing: Async -> Thread -> Process -> Interpreter -> Async"""
    return [
        StageConfig(name="fetch", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
        StageConfig(name="thread", execution_type=ExecutionType.THREAD, workers=1, queue_size=5),
        StageConfig(name="process", execution_type=ExecutionType.PROCESS, workers=1, queue_size=5),
        StageConfig(name="interpret", execution_type=ExecutionType.INTERPRETER, workers=1, queue_size=5),
        StageConfig(name="store", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
    ]


# Picklable handler functions for Process and Interpreter executors
def _picklable_process_handler(job_id: str, data: Any) -> Any:
    """Picklable sync handler for process/interpreter stages"""
    # Extract stage name from data prefix if present, otherwise use generic prefix
    return f"process_{data}"


def _picklable_interpreter_handler(job_id: str, data: Any) -> Any:
    """Picklable sync handler for interpreter stages"""
    return f"interpreter_{data}"


class MockETLPipeline(ETLPipeline):
    """
    Mock implementation of ETLPipeline for testing purposes.

    This class overrides the abstract methods to provide mock behavior.
    You can call mock methods on mock_refill_queue and stage handlers.

    By default creates a 5-stage pipeline: fetch -> thread -> process -> interpret -> store
    with different execution types (async, thread, process, interpreter, async).

    Note: Process and Interpreter stages cannot use mocks (not picklable), so they use real functions.
    """

    def _validate_handler_signature(self, stage_name: str, stage_config: StageConfig, handler: Any) -> None:
        """Override validation to allow mock handlers with *args, **kwargs."""
        # Skip validation for mock pipeline - mocks have *args, **kwargs which would fail validation
        pass

    def __init__(self, mocker: MockerFixture, config: MockETLPipelineConfig) -> None:
        """Initialize the mock ETL pipeline with configuration."""
        # Use default stages if none provided
        stages = config.stages if config.stages is not None else create_default_stages()

        # Pass ETL pipeline parameters to parent constructor
        etl_config = ETLPipelineConfig(
            pipeline_name=config.pipeline_name,
            stages=stages,
            queue_refresh_rate=config.queue_refresh_rate,
            stats_interval_seconds=config.stats_interval_seconds,
            enable_tracking=config.enable_tracking,
        )

        super().__init__(config=etl_config)

        # Store configuration
        self.config = config
        self.stages = stages

        # Store items to process
        self.items_to_process: list[WorkItem] = config.items_to_process or []

        # Create mocks for refill_queue
        self.mock_refill_queue = mocker.AsyncMock(side_effect=self._refill_queue_impl)

        # Create mock handlers for each stage
        self.mock_stage_handlers: dict[str, Any] = {}
        for stage in stages:
            # Process and Interpreter executors require picklable functions, so use real functions
            if stage.execution_type == ExecutionType.PROCESS:
                # Use picklable function for process stage
                handler = _picklable_process_handler
                self.mock_stage_handlers[stage.name] = handler
            elif stage.execution_type == ExecutionType.INTERPRETER:
                # Use picklable function for interpreter stage
                handler = _picklable_interpreter_handler
                self.mock_stage_handlers[stage.name] = handler
            elif stage.execution_type == ExecutionType.ASYNC:
                mock_handler = mocker.AsyncMock(side_effect=self._create_async_handler_impl(stage.name))
                self.mock_stage_handlers[stage.name] = mock_handler
                handler = mock_handler
            else:  # THREAD
                mock_handler = mocker.Mock(side_effect=self._create_thread_handler_impl(stage.name))
                self.mock_stage_handlers[stage.name] = mock_handler
                handler = mock_handler

            self.register_stage_handler(stage.name, handler)

        # Replace the refill_queue method with mock
        self.refill_queue = self.mock_refill_queue

    async def _refill_queue_impl(self, count: int) -> list[WorkItem]:
        """Implementation of refill_queue for testing"""
        if self.items_to_process:
            batch = self.items_to_process[:count]
            self.items_to_process = self.items_to_process[count:]
            return batch
        return []

    def _create_async_handler_impl(self, stage_name: str) -> Any:
        """Create an async handler implementation for a stage"""

        async def handler_impl(item: WorkItem) -> WorkItem:
            if self.config.process_sleep is not None and self.config.process_sleep > 0:
                await asyncio.sleep(self.config.process_sleep)
            item.data = f"{stage_name}_{item.data}"
            return item

        return handler_impl

    def _create_thread_handler_impl(self, stage_name: str) -> Any:
        """Create a sync handler implementation for a stage"""

        def handler_impl(item: WorkItem) -> Any:
            if self.config.process_sleep is not None and self.config.process_sleep > 0:
                time.sleep(self.config.process_sleep)
            item.data = f"{stage_name}_{item.data}"
            return item

        return handler_impl


class MockETLPipelineFactory:
    """Collection of factory methods to create MockETLPipeline instances."""

    def __init__(self, mocker: MockerFixture) -> None:
        """Initialize the mocker."""
        self._mocker = mocker

    def create(
        self,
        config: MockETLPipelineConfig | None = None,
        work_items_count: int | None = None,
        stages: list[StageConfig] | None = None,
        **kwargs: Any,
    ) -> MockETLPipeline:
        """
        Create a MockETLPipeline with the given configuration.

        Will always overwrite items_to_process in config if work_items_count is passed.
        Will ignore kwargs if config is passed.

        Args:
            config: Mock pipeline configuration
            work_items_count: Number of work items to create
            stages: Custom stages configuration (overrides default 5 stages)
            **kwargs: Additional config parameters

        """
        if config is None:
            config = MockETLPipelineConfig(**kwargs)
        if stages is not None:
            config.stages = stages
        if work_items_count:
            work_items = work_item_factory(count=work_items_count)
            config.items_to_process = work_items
        return MockETLPipeline(self._mocker, config)


@pytest.fixture
def mock_etl_pipeline_factory(mocker: MockerFixture) -> MockETLPipelineFactory:
    """Fixture providing a MockETLPipelineFactory for creating test pipelines"""
    return MockETLPipelineFactory(mocker)
