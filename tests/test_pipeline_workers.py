"""Tests for the pipeline workers."""

import pytest
from bergtalzug import WorkItem
from conftest import MockETLPipelineFactory


class TestPipelineWorkers:
    """Test individual worker components"""

    @pytest.mark.asyncio
    async def test_fetch_worker_flow(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test fetch worker processes items correctly"""
        pipeline = mock_etl_pipeline_factory.create()
        await pipeline._setup_queues()

        item = WorkItem(data="test")
        await pipeline._fetch_queue.async_put(item)
        await pipeline._fetch_queue.async_put(None)  # Poison pill

        await pipeline._fetch_worker()

        pipeline.mock_fetch.assert_called_once_with(item)
        item_to_process = await pipeline._process_queue.async_get()
        assert item_to_process is not None

    @pytest.mark.asyncio
    async def test_process_worker_flow(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test process worker in processes items correctly"""
        pipeline = mock_etl_pipeline_factory.create()
        await pipeline._setup_queues()

        item = WorkItem(data="test")
        await pipeline._process_queue.async_put(item)
        await pipeline._process_queue.async_put(None)  # Poison pill

        await pipeline._process_worker()

        pipeline.mock_process.assert_called_once_with(item)
        item_to_store = await pipeline._store_queue.async_get()
        assert item_to_store is not None
        assert item_to_store.data == "processed_test"

    @pytest.mark.asyncio
    async def test_sync_process_worker_flow(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test process worker in processes items correctly"""
        pipeline = mock_etl_pipeline_factory.create_sync()
        await pipeline._setup_queues()

        item = WorkItem(data="test")
        await pipeline._process_queue.async_put(item)
        await pipeline._process_queue.async_put(None)  # Poison pill

        await pipeline._process_worker()

        pipeline.mock_sync_process.assert_called_once_with(item)
        item_to_store = await pipeline._store_queue.async_get()
        assert item_to_store is not None
        assert item_to_store.data == "processed_test"

    @pytest.mark.asyncio
    async def test_store_worker_flow(self, mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
        """Test store worker processes items"""
        # Disable tracking so that we don't have to deal with job Id correctness check
        pipeline = mock_etl_pipeline_factory.create(enable_tracking=False)
        await pipeline._setup_queues()

        item = WorkItem(data="test")
        await pipeline._store_queue.async_put(item)
        await pipeline._store_queue.async_put(None)  # Poison pill

        await pipeline._store_worker()
        pipeline.mock_store.assert_called_once_with(item)
