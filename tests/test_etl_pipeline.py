"""Tests for the complete ETL pipeline integration."""

import asyncio
import pytest
from conftest import work_item_factory, MockETLPipelineFactory, create_default_stages
from bergtalzug.etl_pipeline import StageConfig, ExecutionType, WorkItem
from pytest_mock import MockerFixture


class TestPipelineIntegration:
    """Test complete pipeline flow"""

    @pytest.mark.asyncio
    async def test_pipeline_starts_and_dies_naturally(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test that pipeline starts and finishes when there are no items to process."""
        # Create pipeline instance with no items to process
        pipeline = mock_etl_pipeline_factory.create()

        assert pipeline.items_to_process == []  # pipeline should start with no items
        await pipeline.run()
        assert pipeline.items_to_process == []  # still empty after run

        # Verify refill_queue was called at least once to check for items
        pipeline.mock_refill_queue.assert_called()

    @pytest.mark.asyncio
    async def test_simple_pipeline_flow(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test basic end-to-end flow with default 5-stage pipeline"""
        pipeline = mock_etl_pipeline_factory.create(work_items_count=5)
        results = await pipeline.run()

        # Verify all items processed
        assert len(results) == 5
        assert all(r.success for r in results)

        # Verify mockable stages were called (async and thread stages can be mocked)
        # Process and Interpreter stages use picklable functions and can't be mocked
        assert pipeline.mock_stage_handlers["fetch"].call_count == 5
        assert pipeline.mock_stage_handlers["thread"].call_count == 5
        # process (process) and interpret (interpreter) are not mocked
        assert pipeline.mock_stage_handlers["store"].call_count == 5

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("stage_configs", "expected_count"),
        [
            # Simple three stages with PROCESS
            (
                [
                    StageConfig(name="fetch", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
                    StageConfig(name="process", execution_type=ExecutionType.PROCESS, workers=1, queue_size=5),
                    StageConfig(name="store", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
                ],
                100,
            ),
            # Mixed sequence with THREAD and INTERPRETER
            (
                [
                    StageConfig(name="fetch", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
                    StageConfig(name="thread", execution_type=ExecutionType.THREAD, workers=2, queue_size=5),
                    StageConfig(name="interpret", execution_type=ExecutionType.INTERPRETER, workers=2, queue_size=5),
                    StageConfig(name="store", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
                ],
                150,
            ),
            # Alternating all four types
            (
                [
                    StageConfig(name="async_one", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
                    StageConfig(name="thread", execution_type=ExecutionType.THREAD, workers=1, queue_size=5),
                    StageConfig(name="process", execution_type=ExecutionType.PROCESS, workers=1, queue_size=5),
                    StageConfig(name="interpret", execution_type=ExecutionType.INTERPRETER, workers=1, queue_size=5),
                    StageConfig(name="async_two", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
                ],
                120,
            ),
            # Multiple PROCESS and INTERPRETER stages
            (
                [
                    StageConfig(name="fetch", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
                    StageConfig(name="process_one", execution_type=ExecutionType.PROCESS, workers=2, queue_size=10),
                    StageConfig(
                        name="interpret_one", execution_type=ExecutionType.INTERPRETER, workers=2, queue_size=10
                    ),
                    StageConfig(name="process_two", execution_type=ExecutionType.PROCESS, workers=1, queue_size=5),
                    StageConfig(name="store", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
                ],
                150,
            ),
            # Varying queue sizes with mixed types
            (
                [
                    StageConfig(name="fetch", execution_type=ExecutionType.ASYNC, workers=1, queue_size=50),
                    StageConfig(name="process", execution_type=ExecutionType.PROCESS, workers=2, queue_size=10),
                    StageConfig(name="thread", execution_type=ExecutionType.THREAD, workers=1, queue_size=5),
                    StageConfig(name="store", execution_type=ExecutionType.ASYNC, workers=1, queue_size=20),
                ],
                200,
            ),
            # High concurrency with all four types
            (
                [
                    StageConfig(name="fetch", execution_type=ExecutionType.ASYNC, workers=30, queue_size=1000),
                    StageConfig(name="thread", execution_type=ExecutionType.THREAD, workers=30, queue_size=1000),
                    StageConfig(name="process", execution_type=ExecutionType.PROCESS, workers=30, queue_size=1000),
                    StageConfig(
                        name="interpret", execution_type=ExecutionType.INTERPRETER, workers=30, queue_size=1000
                    ),
                    StageConfig(name="async", execution_type=ExecutionType.ASYNC, workers=30, queue_size=1000),
                    StageConfig(name="store", execution_type=ExecutionType.ASYNC, workers=30, queue_size=1000),
                ],
                1500,
            ),
        ],
    )
    async def test_pipeline_with_custom_stages(
        self,
        mock_etl_pipeline_factory: MockETLPipelineFactory,
        stage_configs: list[StageConfig],
        expected_count: int,
    ) -> None:
        """Test pipeline with various stage configurations and orderings"""
        pipeline = mock_etl_pipeline_factory.create(work_items_count=expected_count, stages=stage_configs)
        results = await pipeline.run()

        # Verify all items processed
        assert len(results) == expected_count
        assert all(r.success for r in results)

        # Verify mockable stages were called (PROCESS and INTERPRETER use picklable functions, not mocks)
        for stage_config in stage_configs:
            handler = pipeline.mock_stage_handlers[stage_config.name]
            # Only verify call_count for mockable stages (ASYNC and THREAD)
            if stage_config.execution_type in (ExecutionType.ASYNC, ExecutionType.THREAD):
                assert handler.call_count == expected_count

    @pytest.mark.asyncio
    async def test_simple_pipeline_with_tracking_disabled(
        self, mock_etl_pipeline_factory: MockETLPipelineFactory
    ) -> None:
        """Test basic end-to-end flow with tracking disabled"""
        pipeline = mock_etl_pipeline_factory.create(work_items_count=15, enable_tracking=False)

        # Without item tracking no pipeline results
        await pipeline.run()

        # Verify mockable stages were called
        assert pipeline.mock_stage_handlers["fetch"].call_count == 15
        assert pipeline.mock_stage_handlers["thread"].call_count == 15
        # process (process) and interpret (interpreter) are not mocked
        assert pipeline.mock_stage_handlers["store"].call_count == 15

    @pytest.mark.asyncio
    async def test_pipeline_with_stats_disabled(
        self, mock_etl_pipeline_factory: MockETLPipelineFactory, mocker: MockerFixture
    ) -> None:
        """Test that stats are not reported when stats_interval_seconds=0"""
        pipeline = mock_etl_pipeline_factory.create(work_items_count=10, stats_interval_seconds=0.0)

        # Mock the tracker's log_statistics method to verify it's not called
        log_stats_spy = mocker.spy(pipeline.tracker, "log_statistics")

        results = await pipeline.run()

        assert len(results) == 10
        assert all(r.success for r in results)

        # Verify that log_statistics was never called during periodic reporting
        assert log_stats_spy.call_count == 0

    @pytest.mark.asyncio
    async def test_pipeline_with_errors(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test pipeline handles errors gracefully"""
        items = work_item_factory(count=3)
        pipeline = mock_etl_pipeline_factory.create(items_to_process=items)

        # Make second item fail during the thread stage (which is a THREAD stage - sync handler)
        def thread_with_error(work_item: WorkItem) -> WorkItem:
            if work_item.job_id == items[1].job_id:
                raise ValueError("Processing error")
            work_item.data = f"thread_{work_item.data}"
            return work_item

        pipeline.mock_stage_handlers["thread"].side_effect = thread_with_error

        results = await pipeline.run()

        # Check that error was handled
        success_count = sum(1 for r in results if r.success)
        error_count = sum(1 for r in results if not r.success)

        assert success_count == 2
        assert error_count == 1

    @pytest.mark.asyncio
    async def test_pipeline_queue_management(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test queue refill mechanism"""
        # Create custom stages with smaller queue for first stage
        custom_stages = create_default_stages()
        custom_stages[0].queue_size = 5  # Small queue for fetch stage

        pipeline = mock_etl_pipeline_factory.create(work_items_count=20, stages=custom_stages)
        await pipeline.run()

        # Verify refill_queue was called multiple times:
        # currently queue always refills to 90%.
        # That is 4 items per refill with a queue size of 5.
        # 20 items / 4 items per refill = 5 refills
        # At the end one more to initiate shutdown
        assert pipeline.mock_refill_queue.call_count == 6

    @pytest.mark.asyncio
    async def test_pipeline_updates_tracker_stages(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test that pipeline correctly updates tracker stages"""
        pipeline = mock_etl_pipeline_factory.create(work_items_count=100, process_sleep=0.01)
        assert pipeline.tracker is not None
        tracker = pipeline.tracker

        # Start pipeline but don't await yet
        await pipeline.start()

        # Give it a moment to start processing but not finish
        await asyncio.sleep(0.05)

        # Check that items are in progress (not all completed yet)
        stats = await tracker.get_statistics()
        # With 100 items and 5 stages, not all should be done yet
        assert stats["active_items"] > 0 or stats["completed_items"] < 100

        # Wait for completion
        await pipeline.run()

        # Verify all items completed
        final_stats = await tracker.get_statistics()
        assert final_stats["active_items"] == 0
        assert final_stats["completed_items"] == 100
        assert final_stats["success_rate"] == 1.0

    @pytest.mark.asyncio
    async def test_pipeline_not_properly_initialized_error(
        self, mock_etl_pipeline_factory: MockETLPipelineFactory
    ) -> None:
        """Test that run() raises RuntimeError if pipeline is not properly initialized"""
        pipeline = mock_etl_pipeline_factory.create(work_items_count=5)

        # Set pipeline to running
        pipeline._running = True

        # Verify that run() raises RuntimeError with the expected message
        with pytest.raises(RuntimeError, match="Pipeline not properly initialized"):
            await pipeline.run()
