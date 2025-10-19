"""Tests for the ItemTracker class."""

import pytest
import asyncio
from typing import cast
from bergtalzug import ItemTracker, WorkItem, PipelineStage, PipelineResult


class TestItemTracker:
    """Test item tracking functionality"""

    @pytest.mark.asyncio
    async def test_register_item(self) -> None:
        """Test registering new items"""
        tracker = ItemTracker()
        item = WorkItem(data="test")

        await tracker.register_item(item)
        active_items = await tracker.get_active_items()

        assert len(active_items) == 1
        assert active_items[0].job_id == item.job_id

    @pytest.mark.asyncio
    async def test_create_and_update_item_stage(self) -> None:
        """Test updating item stages"""
        tracker = ItemTracker()
        item = WorkItem(data="test")

        await tracker.register_item(item)

        active = await tracker.get_active_items()
        assert active[0].metadata.stage == PipelineStage.CREATED

        await tracker.update_item_stage(item.job_id, PipelineStage.FETCHING)

        active = await tracker.get_active_items()
        assert len(active) == 1
        assert active[0].metadata.stage == PipelineStage.FETCHING

    @pytest.mark.asyncio
    async def test_complete_item_success(self) -> None:
        """Test completing items successfully"""
        tracker = ItemTracker()
        item = WorkItem(data="test")

        await tracker.register_item(item)
        result = await tracker.complete_item(item.job_id, success=True)

        assert result is not None
        assert result.success is True
        assert result.job_id == item.job_id

        # Item should be moved from active to completed
        active = await tracker.get_active_items()
        completed = await tracker.get_completed_results()
        assert len(active) == 0
        assert len(completed) == 1

    @pytest.mark.asyncio
    async def test_complete_item_with_error(self) -> None:
        """Test completing items with errors"""
        tracker = ItemTracker()
        item = WorkItem(data="test")
        error = ValueError("Test error")

        await tracker.register_item(item)
        result = await tracker.complete_item(item.job_id, success=False, error=error)

        assert result is not None
        assert result.success is False
        assert result.error == error
        assert result.metadata.stage == PipelineStage.ERROR

    @pytest.mark.asyncio
    async def test_completion_callbacks(self) -> None:
        """Test completion callbacks are triggered"""
        tracker = ItemTracker()
        callback_called = False
        callback_result: PipelineResult | None = None

        def callback(result: PipelineResult) -> None:
            nonlocal callback_called, callback_result
            callback_called = True
            callback_result = result

        tracker.add_completion_callback(callback)

        item = WorkItem(data="test")
        await tracker.register_item(item)
        await tracker.complete_item(item.job_id)

        assert callback_called is True

        result = cast(PipelineResult, callback_result)
        assert result.job_id == item.job_id

    @pytest.mark.asyncio
    async def test_get_statistics(self) -> None:
        """Test statistics calculation"""
        tracker = ItemTracker()

        # Add items in different stages
        for i in range(3):
            item = WorkItem(data=f"test{i}".encode())
            await tracker.register_item(item)
            if i == 0:
                await tracker.update_item_stage(item.job_id, PipelineStage.FETCHING)
            elif i == 1:
                await tracker.update_item_stage(item.job_id, PipelineStage.PROCESSING)

        # Complete one item
        completed_item = WorkItem(data="completed")
        await tracker.register_item(completed_item)
        await tracker.complete_item(completed_item.job_id, success=True)

        stats = await tracker.get_statistics()

        assert stats["active_items"] == 3
        assert stats["completed_items"] == 1
        assert stats["success_rate"] == 1.0
        assert "fetching" in stats["active_by_stage"]

    @pytest.mark.asyncio
    async def test_concurrent_access(self) -> None:
        """Test thread-safe concurrent access"""
        tracker = ItemTracker()

        async def add_items(start_idx: int, count: int) -> None:
            for i in range(start_idx, start_idx + count):
                item = WorkItem(data=f"item{i}".encode())
                await tracker.register_item(item)
                await tracker.update_item_stage(item.job_id, PipelineStage.PROCESSING)

        # Run multiple coroutines concurrently
        await asyncio.gather(add_items(0, 10), add_items(10, 10), add_items(20, 10))

        active_items = await tracker.get_active_items()
        assert len(active_items) == 30

    @pytest.mark.asyncio
    async def test_nonexistent_job_id(self) -> None:
        """Test completing an item with a nonexistent job ID"""
        tracker = ItemTracker()
        item = WorkItem(data="test")

        with pytest.raises(RuntimeError):
            await tracker.complete_item(item.job_id)
