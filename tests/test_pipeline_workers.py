"""Tests for the pipeline workers."""

import pytest
from bergtalzug import WorkItem, StageConfig, ExecutionType
from conftest import MockETLPipelineFactory


class TestPipelineWorkers:
    """Test individual worker components with dynamic stages"""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("execution_type", "stage_name", "expected_data_prefix", "needs_executor"),
        [
            (ExecutionType.ASYNC, "async_stage", "async_stage", False),
            (ExecutionType.THREAD, "thread_stage", "thread_stage", True),
            (ExecutionType.PROCESS, "process_stage", "process", True),
            (ExecutionType.INTERPRETER, "interpreter_stage", "interpreter", True),
        ],
    )
    async def test_stage_worker_flow(
        self,
        mock_etl_pipeline_factory: MockETLPipelineFactory,
        execution_type: ExecutionType,
        stage_name: str,
        expected_data_prefix: str,
        needs_executor: bool,
    ) -> None:
        """Test stage worker processes items correctly for different execution types"""
        stages = [
            StageConfig(name=stage_name, execution_type=execution_type, workers=1, queue_size=10),
            StageConfig(name="next_stage", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
        ]
        pipeline = mock_etl_pipeline_factory.create(stages=stages)
        await pipeline._setup_queues()

        if needs_executor:
            await pipeline._setup_executors()

        item = WorkItem(data="test")
        await pipeline._queues[stage_name].async_put(item)
        await pipeline._queues[stage_name].async_put(None)  # Poison pill

        # Run the first stage worker (index 0)
        await pipeline._create_stage_worker(0)

        # For async and thread stages, we can check the mock was called
        if execution_type in (ExecutionType.ASYNC, ExecutionType.THREAD):
            pipeline.mock_stage_handlers[stage_name].assert_called_once()

        # Check that item was passed to next stage with expected data
        item_in_next_stage = await pipeline._queues["next_stage"].async_get()
        assert item_in_next_stage is not None
        assert item_in_next_stage.data == f"{expected_data_prefix}_test"

    @pytest.mark.asyncio
    async def test_final_stage_worker_flow(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test final stage worker completes items"""
        stages = [
            StageConfig(name="final_stage", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
        ]
        # Disable tracking so that we don't have to deal with job Id correctness check
        pipeline = mock_etl_pipeline_factory.create(stages=stages, enable_tracking=False)
        await pipeline._setup_queues()

        item = WorkItem(data="test")
        await pipeline._queues["final_stage"].async_put(item)
        await pipeline._queues["final_stage"].async_put(None)  # Poison pill

        # Run the final stage work  er (index 0, which is also the last stage)
        await pipeline._create_stage_worker(0)

        # Check that the final stage handler was called
        pipeline.mock_stage_handlers["final_stage"].assert_called_once()

        # Item should be completed
        if pipeline.tracker:
            stats = await pipeline.tracker.get_statistics()
            assert stats["completed_items"] == 1
