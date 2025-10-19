"""Tests for the Config class."""

import pytest
from typing import Any
from conftest import MockETLPipelineFactory
from pydantic import ValidationError
import random
from bergtalzug import ETLPipelineConfig, ETLPipeline, WorkItem


@pytest.mark.parametrize(
    ("field_name", "valid_values"),
    [
        ("pipeline_name", ["test_pipeline", "", "my-etl"]),
        ("fetch_workers", [1, 100, 1000]),
        ("process_workers", [1, 100, 1000]),
        ("store_workers", [1, 100, 1000]),
        ("fetch_queue_size", [1, 500, 10000]),
        ("process_queue_size", [1, 500, 10000]),
        ("store_queue_size", [1, 500, 10000]),
        ("queue_refresh_rate", [0.0, 0.1, 1.0, 100.5]),
        ("stats_interval_seconds", [0.0, 0.1, 1.0, 100.5]),
        ("enable_tracking", [True, False]),
    ],
)
def test_config_valid_values(field_name: str, valid_values: list[Any]) -> None:
    """Test that valid values are accepted."""
    for value in valid_values:
        config = ETLPipelineConfig(**{field_name: value})
        assert getattr(config, field_name) == value


@pytest.mark.parametrize(
    ("field_name", "invalid_values"),
    [
        ("pipeline_name", [123, True, None]),
        ("fetch_workers", [0, -1, 1.5, "10", None]),
        ("process_workers", [0, -1, 1.5, "10", None]),
        ("store_workers", [0, -1, 1.5, "10", None]),
        ("fetch_queue_size", [0, -1, 1.5, "1000", None]),
        ("process_queue_size", [0, -1, 1.5, "1000", None]),
        ("store_queue_size", [0, -1, 1.5, "1000", None]),
        ("queue_refresh_rate", [-0.1, -1, "1", None]),
        ("stats_interval_seconds", [-0.1, -1, "1", None]),
        ("enable_tracking", ["True", None]),
    ],
)
def test_config_invalid_values(field_name: str, invalid_values: list[Any]) -> None:
    """Test that invalid values raise ValidationError."""
    for value in invalid_values:
        with pytest.raises(ValidationError):
            ETLPipelineConfig(**{field_name: value})


@pytest.mark.parametrize(
    ("config_params", "error"),
    [
        # Combined valid parameters
        (
            {
                "pipeline_name": "test",
                "fetch_workers": 20,
                "process_workers": 10,
                "store_workers": 15,
            },
            None,
        ),
        (
            {
                "fetch_queue_size": 2000,
                "process_queue_size": 1000,
                "store_queue_size": 2000,
                "queue_refresh_rate": 2.5,
            },
            None,
        ),
        (
            {
                "pipeline_name": "production_etl",
                "enable_tracking": False,
                "stats_interval_seconds": 30.0,
            },
            None,
        ),
        (
            {
                "fetch_workers": 1,
                "process_workers": 1,
                "store_workers": 1,
                "fetch_queue_size": 1,
                "process_queue_size": 1,
                "store_queue_size": 1,
            },
            None,
        ),
        # Invalid combinations
        (
            {
                "fetch_workers": 0,
                "process_workers": 0,
            },
            ValidationError,
        ),
        (
            {
                "pipeline_name": None,
                "enable_tracking": "yes",
            },
            ValidationError,
        ),
    ],
)
def test_etl_pipeline_config_validation(config_params: dict[str, Any], error: type[ValidationError] | None) -> None:
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
            assert getattr(config, key) == value


def test_process_is_sync_set_to_true_when_sync_process_defined() -> None:
    """Test that _process_is_sync is True when sync_process is implemented."""

    class TestSyncPipeline(ETLPipeline):  # pragma: no cover
        async def refill_queue(self, count: int) -> list[WorkItem]:
            """Empty fetch"""
            return [WorkItem("test")]

        async def fetch(self, item: WorkItem) -> WorkItem:
            """Empty fetch"""
            return item

        def sync_process(self, item: Any) -> Any:
            """Dummy sync process method."""
            return item

        async def store(self, item: WorkItem) -> None:
            """Empty store"""
            pass

    pipeline = TestSyncPipeline()

    # Assert that _process_is_sync is set to True
    assert pipeline._process_is_sync is True


def test_process_is_sync_set_to_false_when_sync_process_not_defined() -> None:
    """Test that _process_is_sync is False when process is implemented."""

    class TestSyncPipeline(ETLPipeline):  # pragma: no cover
        async def refill_queue(self, count: int) -> list[WorkItem]:
            """Empty fetch"""
            return [WorkItem("test")]

        async def fetch(self, item: WorkItem) -> WorkItem:
            """Empty fetch"""
            return item

        async def process(self, item: Any) -> Any:
            """Dummy async process method."""
            return item

        async def store(self, item: WorkItem) -> None:
            """Empty store"""
            pass

    pipeline = TestSyncPipeline()

    # Assert that _process_is_sync is set to False
    assert pipeline._process_is_sync is False


@pytest.mark.parametrize("count", [random.randint(1, 50) for _ in range(10)])
def test_thread_pool_executor_size_matches_process_workers(
    mock_etl_pipeline_factory: MockETLPipelineFactory, count: int
) -> None:
    """Test that ThreadPoolExecutor max_workers matches process_workers config when using sync_process."""
    # Test with different process_workers values
    pipeline = mock_etl_pipeline_factory.create_sync(process_workers=count)

    # Verify that _process_executor is created and has correct max_workers
    assert pipeline._process_executor is not None
    assert pipeline._process_executor._max_workers == count


def test_no_thread_pool_executor_when_async_process(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test that ThreadPoolExecutor is not created when using async process."""
    pipeline = mock_etl_pipeline_factory.create(process_workers=10)

    # Verify that _process_executor is None
    assert pipeline._process_executor is None
