"""Tests for handler signature validation."""

import pytest
from typing import Any
from conftest import MockETLPipelineFactory
from bergtalzug.etl_pipeline import StageConfig, ExecutionType, WorkItem, ETLPipeline, ETLPipelineConfig


class SimplePipeline(ETLPipeline):
    """Simple pipeline for testing handler validation."""

    async def refill_queue(self, count: int) -> list[WorkItem]:
        """Return empty list - no items to process."""
        return []


class TestPipelineHandlerValidation:
    """Test pipeline validation and error handling"""

    @pytest.mark.asyncio
    async def test_register_stage_handler_invalid_stage_name(
        self, mock_etl_pipeline_factory: MockETLPipelineFactory
    ) -> None:
        """Test that registering a handler for a non-existent stage raises ValueError"""
        pipeline = mock_etl_pipeline_factory.create()

        # Try to register a handler for a stage that doesn't exist in the configuration
        async def invalid_handler(work_item: WorkItem) -> WorkItem:
            return work_item

        with pytest.raises(ValueError, match="Stage 'nonexistent_stage' not found in pipeline configuration"):
            pipeline.register_stage_handler("nonexistent_stage", invalid_handler)

    @pytest.mark.parametrize(
        ("execution_type", "is_handler_async", "error_match"),
        [
            (ExecutionType.ASYNC, False, "execution_type=ASYNC but handler is not async"),
            (ExecutionType.THREAD, True, "execution_type=thread but handler is async"),
            (ExecutionType.PROCESS, True, "execution_type=process but handler is async"),
            (ExecutionType.INTERPRETER, True, "execution_type=interpreter but handler is async"),
        ],
        ids=[
            "async_stage_sync_handler",
            "thread_stage_async_handler",
            "process_stage_async_handler",
            "interpreter_stage_async_handler",
        ],
    )
    def test_execution_type_mismatch_raises_error(
        self, execution_type: ExecutionType, is_handler_async: bool, error_match: str
    ) -> None:
        """Test that mismatched handler types raise appropriate errors."""
        config = ETLPipelineConfig(
            stages=[StageConfig(name="stage1", execution_type=execution_type)],
        )
        pipeline = SimplePipeline(config)

        # Create handler based on parameters
        if is_handler_async:
            if execution_type == ExecutionType.PROCESS:

                async def async_process_handler(job_id: str, data: Any) -> Any:
                    return data

                handler = async_process_handler
            else:

                async def async_handler(item: WorkItem) -> WorkItem:
                    return item

                handler = async_handler
        else:

            def sync_handler(item: WorkItem) -> WorkItem:
                return item

            handler = sync_handler

        with pytest.raises(TypeError, match=error_match):
            pipeline.register_stage_handler("stage1", handler)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("execution_type", "param_count", "expected_count", "handler_def"),
        [
            (ExecutionType.ASYNC, 2, 1, "async def wrong_handler(item: WorkItem, extra: int) -> WorkItem: return item"),
            (ExecutionType.THREAD, 0, 1, "def wrong_handler() -> WorkItem: return WorkItem(data={})"),
            (ExecutionType.PROCESS, 1, 2, "def wrong_handler(job_id: str) -> Any: return {}"),
            (
                ExecutionType.INTERPRETER,
                3,
                2,
                "def wrong_handler(job_id: str, data: Any, extra: int) -> Any: return data",
            ),
        ],
        ids=["async_wrong_params", "thread_wrong_params", "process_wrong_params", "interpreter_wrong_params"],
    )
    def test_wrong_param_count_raises_error(
        self, execution_type: ExecutionType, param_count: int, expected_count: int, handler_def: str
    ) -> None:
        """Test that handlers with wrong parameter counts raise appropriate errors."""
        config = ETLPipelineConfig(
            pipeline_name="test",
            stages=[StageConfig(name="stage1", execution_type=execution_type)],
        )
        pipeline = SimplePipeline(config)

        # Create handler with wrong number of params
        local_vars: dict[str, Any] = {"WorkItem": WorkItem, "Any": Any}
        exec(handler_def, local_vars)  # dynamically create function
        wrong_handler = local_vars["wrong_handler"]

        error_pattern = rf"must accept exactly {expected_count} parameter.*got {param_count} parameters"
        with pytest.raises(TypeError, match=error_pattern):
            pipeline.register_stage_handler("stage1", wrong_handler)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("execution_type", "handler_def"),
        [
            (ExecutionType.ASYNC, "async def valid_handler(item: WorkItem) -> WorkItem: return item"),
            (ExecutionType.THREAD, "def valid_handler(item: WorkItem) -> WorkItem: return item"),
            (ExecutionType.PROCESS, "def valid_handler(job_id: str, data: Any) -> Any: return data"),
            (ExecutionType.INTERPRETER, "def valid_handler(job_id: str, data: Any) -> Any: return data"),
        ],
        ids=["valid_async", "valid_thread", "valid_process", "valid_interpreter"],
    )
    def test_valid_handler_registers_successfully(self, execution_type: ExecutionType, handler_def: str) -> None:
        """Test that valid handlers register without errors."""
        config = ETLPipelineConfig(
            pipeline_name="test",
            stages=[StageConfig(name="stage1", execution_type=execution_type)],
        )
        pipeline = SimplePipeline(config)

        # Create valid handler
        local_vars: dict[str, Any] = {"WorkItem": WorkItem, "Any": Any}
        exec(handler_def, local_vars)  # dynamically create function
        valid_handler = local_vars["valid_handler"]

        # Should not raise
        pipeline.register_stage_handler("stage1", valid_handler)

    def test_nonexistent_stage_name_raises_error(self) -> None:
        """Registering handler for non-existent stage should fail."""
        config = ETLPipelineConfig(
            pipeline_name="test",
            stages=[StageConfig(name="stage1", execution_type=ExecutionType.ASYNC)],
        )
        pipeline = SimplePipeline(config)

        async def handler(item: WorkItem) -> WorkItem:
            return item

        with pytest.raises(ValueError, match="Stage 'nonexistent' not found"):
            pipeline.register_stage_handler("nonexistent", handler)
