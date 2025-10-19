"""Tests for the various data models."""

from datetime import timedelta
from bergtalzug import WorkItem, WorkItemMetadata, PipelineStage, PipelineResult


class TestWorkItemMetadata:
    """Test WorkItemMetadata lifecycle tracking"""

    def test_initial_state(self) -> None:
        """Test metadata is initialized correctly"""
        metadata = WorkItemMetadata()
        assert metadata.stage == PipelineStage.CREATED
        assert metadata.fetch_started_at is None
        assert metadata.error_details is None

    def test_stage_transitions(self) -> None:
        """Test stage transitions are recorded properly"""
        metadata = WorkItemMetadata()
        metadata.add_stage_transition(PipelineStage.FETCHING)
        assert metadata.stage == PipelineStage.FETCHING

    def test_custom_metadata(self) -> None:
        """Test custom metadata storage"""
        metadata = WorkItemMetadata()
        metadata.custom_metadata["key"] = "value"
        assert metadata.custom_metadata["key"] == "value"


class TestWorkItem:
    """Test WorkItem creation and properties"""

    def test_work_item_creation(self) -> None:
        """Test WorkItem is created with proper defaults"""
        item = WorkItem(data="test")
        assert item.data == "test"
        assert item.job_id is not None
        assert isinstance(item.metadata, WorkItemMetadata)

    def test_custom_job_id(self) -> None:
        """Test WorkItem with custom job_id"""
        item = WorkItem(data="test", job_id="custom-123")
        assert item.job_id == "custom-123"


class TestPipelineResult:
    """Test PipelineResult calculations"""

    def test_total_duration_calculation(self) -> None:
        """Test duration is calculated correctly"""
        metadata = WorkItemMetadata()
        wait_time = timedelta(seconds=2)

        # Simulate completion after 2 seconds
        metadata.completed_at = metadata.created_at + wait_time

        result = PipelineResult(job_id="test", success=True, metadata=metadata)
        assert result.total_duration == wait_time.total_seconds()

    def test_total_duration_incomplete(self) -> None:
        """Test duration is None for incomplete items"""
        result = PipelineResult(job_id="test", success=False, metadata=WorkItemMetadata())
        assert result.total_duration is None
