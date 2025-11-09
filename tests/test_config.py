"""Tests for the Config class."""

import pytest
from typing import Any
from conftest import MockETLPipelineFactory
from pydantic import ValidationError
from bergtalzug import ETLPipelineConfig, StageConfig, ExecutionType
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, InterpreterPoolExecutor


@pytest.mark.parametrize(
    ("field_name", "valid_values"),
    [
        ("pipeline_name", ["test_pipeline", "", "my-etl"]),
        ("queue_refresh_rate", [0.0, 0.1, 1.0, 100.5]),
        ("stats_interval_seconds", [0.0, 0.1, 1.0, 100.5]),
        ("enable_tracking", [True, False]),
    ],
)
def test_config_valid_values(field_name: str, valid_values: list[Any]) -> None:
    """Test that valid values are accepted."""
    # Minimal valid stage configuration
    stages = [StageConfig(name="test_stage", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10)]

    for value in valid_values:
        config = ETLPipelineConfig(stages=stages, **{field_name: value})
        assert getattr(config, field_name) == value


@pytest.mark.parametrize(
    ("field_name", "invalid_values"),
    [
        ("pipeline_name", [123, True, None]),
        ("queue_refresh_rate", [-0.1, -1, "1", None]),
        ("stats_interval_seconds", [-0.1, -1, "1", None]),
        ("enable_tracking", ["True", None]),
    ],
)
def test_config_invalid_values(field_name: str, invalid_values: list[Any]) -> None:
    """Test that invalid values raise ValidationError."""
    # Minimal valid stage configuration
    stages = [StageConfig(name="test_stage", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10)]

    for value in invalid_values:
        with pytest.raises(ValidationError):
            ETLPipelineConfig(stages=stages, **{field_name: value})


@pytest.mark.parametrize(
    ("config_params", "error"),
    [
        # Valid single stage
        (
            {
                "pipeline_name": "test",
                "stages": [StageConfig(name="fetch", execution_type=ExecutionType.ASYNC, workers=5, queue_size=100)],
            },
            None,
        ),
        # Valid multi-stage pipeline
        (
            {
                "pipeline_name": "production_etl",
                "stages": [
                    StageConfig(name="fetch", execution_type=ExecutionType.ASYNC, workers=10, queue_size=1000),
                    StageConfig(name="process", execution_type=ExecutionType.THREAD, workers=5, queue_size=500),
                    StageConfig(name="store", execution_type=ExecutionType.ASYNC, workers=3, queue_size=100),
                ],
                "queue_refresh_rate": 2.5,
                "enable_tracking": False,
                "stats_interval_seconds": 30.0,
            },
            None,
        ),
        # All execution types
        (
            {
                "stages": [
                    StageConfig(name="fetch", execution_type=ExecutionType.ASYNC, workers=1, queue_size=10),
                    StageConfig(name="thread_stage", execution_type=ExecutionType.THREAD, workers=2, queue_size=20),
                    StageConfig(name="process_stage", execution_type=ExecutionType.PROCESS, workers=3, queue_size=30),
                    StageConfig(
                        name="interpreter_stage", execution_type=ExecutionType.INTERPRETER, workers=4, queue_size=40
                    ),
                ],
            },
            None,
        ),
        # Minimal config - single stage
        (
            {
                "stages": [StageConfig(name="only_stage", workers=1, queue_size=1)],
            },
            None,
        ),
        # Empty stages list - should fail
        (
            {
                "pipeline_name": "test",
                "stages": [],
            },
            ValidationError,
        ),
        # Duplicate stage names - should fail
        (
            {
                "stages": [
                    StageConfig(name="duplicate", workers=1, queue_size=10),
                    StageConfig(name="duplicate", workers=2, queue_size=20),
                ],
            },
            ValueError,
        ),
        # Invalid parameter combination
        (
            {
                "pipeline_name": None,
                "enable_tracking": "yes",
                "stages": [StageConfig(name="test", workers=1, queue_size=10)],
            },
            ValidationError,
        ),
    ],
)
def test_etl_pipeline_config_validation(config_params: dict[str, Any], error: type[Exception] | None) -> None:
    """Test ETLPipelineConfig validation with various parameter combinations."""
    if error:
        with pytest.raises(error):
            ETLPipelineConfig(**config_params)
    else:
        config = ETLPipelineConfig(**config_params)
        # Verify the config was created successfully
        assert config is not None
        # Verify that provided params were set correctly
        for key, value in config_params.items():
            if key == "stages":
                # For stages, verify length and names match
                assert len(config.stages) == len(value)
                for i, stage_config in enumerate(value):
                    assert config.stages[i].name == stage_config.name
                    assert config.stages[i].workers == stage_config.workers
                    assert config.stages[i].queue_size == stage_config.queue_size
                    assert config.stages[i].execution_type == stage_config.execution_type
            else:
                assert getattr(config, key) == value


def test_stage_config_valid_values() -> None:
    """Test that StageConfig accepts valid values."""
    # Test with all parameters
    stage = StageConfig(name="test_stage", execution_type=ExecutionType.ASYNC, workers=10, queue_size=1000)
    assert stage.name == "test_stage"
    assert stage.execution_type == ExecutionType.ASYNC
    assert stage.workers == 10
    assert stage.queue_size == 1000

    # Test with defaults
    stage_minimal = StageConfig(name="minimal")
    assert stage_minimal.name == "minimal"
    assert stage_minimal.execution_type == ExecutionType.ASYNC
    assert stage_minimal.workers == 5
    assert stage_minimal.queue_size == 1000


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("workers", 0),
        ("workers", -1),
        ("workers", 1.5),
        ("workers", "10"),
        ("queue_size", 0),
        ("queue_size", -1),
        ("queue_size", 1.5),
        ("queue_size", "1000"),
    ],
)
def test_stage_config_invalid_values(field_name: str, invalid_value: Any) -> None:
    """Test that StageConfig rejects invalid values."""
    with pytest.raises(ValidationError):
        StageConfig(name="test", **{field_name: invalid_value})


@pytest.mark.asyncio
async def test_executors_created_for_sync_stages(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test that executors are created for sync stages (THREAD, PROCESS, INTERPRETER)."""
    stages = [
        StageConfig(name="async_stage", execution_type=ExecutionType.ASYNC, workers=2, queue_size=10),
        StageConfig(name="thread_stage", execution_type=ExecutionType.THREAD, workers=3, queue_size=10),
        StageConfig(name="process_stage", execution_type=ExecutionType.PROCESS, workers=4, queue_size=10),
        StageConfig(name="interpreter_stage", execution_type=ExecutionType.INTERPRETER, workers=5, queue_size=10),
    ]

    pipeline = mock_etl_pipeline_factory.create(stages=stages)

    # Setup executors (normally done during start())
    await pipeline._setup_executors()

    # Verify executors dictionary has entries for sync stages only
    assert "async_stage" not in pipeline._executors
    assert "thread_stage" in pipeline._executors
    assert "process_stage" in pipeline._executors
    assert "interpreter_stage" in pipeline._executors

    # Verify executor types
    assert isinstance(pipeline._executors["thread_stage"], ThreadPoolExecutor)
    assert isinstance(pipeline._executors["process_stage"], ProcessPoolExecutor)
    assert isinstance(pipeline._executors["interpreter_stage"], InterpreterPoolExecutor)

    # Verify worker counts for ThreadPoolExecutor (has _max_workers attribute)
    assert pipeline._executors["thread_stage"]._max_workers == 3
    # Note: ProcessPoolExecutor and InterpreterPoolExecutor max_workers checking is handled differently
    # and is tested implicitly through successful executor creation


@pytest.mark.asyncio
async def test_no_executors_for_async_only_pipeline(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test that no executors are created for a pipeline with only async stages."""
    stages = [
        StageConfig(name="fetch", execution_type=ExecutionType.ASYNC, workers=5, queue_size=100),
        StageConfig(name="process", execution_type=ExecutionType.ASYNC, workers=3, queue_size=50),
        StageConfig(name="store", execution_type=ExecutionType.ASYNC, workers=2, queue_size=20),
    ]

    pipeline = mock_etl_pipeline_factory.create(stages=stages)

    # Setup executors (normally done during start())
    await pipeline._setup_executors()

    # Verify that _executors is empty
    assert len(pipeline._executors) == 0
