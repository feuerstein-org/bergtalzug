"""Tests for the various data models."""

from datetime import timedelta
from bergtalzug import WorkItem, WorkItemMetadata, PipelineResult


class TestWorkItemMetadata:
    """Test WorkItemMetadata lifecycle tracking"""

    def test_initial_state(self) -> None:
        """Test metadata is initialized correctly"""
        metadata = WorkItemMetadata()
        assert metadata.current_stage == "created"
        assert metadata.created is not None
        assert metadata.completed is None
        assert metadata.error_details is None
        assert metadata.stage_timings == {}

    def test_stage_transitions(self) -> None:
        """Test stage transitions are recorded properly"""
        metadata = WorkItemMetadata()

        # Test adding a stage transition
        metadata.add_stage_transition("fetching")
        assert metadata.current_stage == "fetching"
        assert "fetching" in metadata.stage_timings
        assert "started" in metadata.stage_timings["fetching"]

        # Test adding a queued transition
        metadata.add_stage_transition("processing", "queued")
        assert metadata.current_stage == "processing"
        assert "queued" in metadata.stage_timings["processing"]

    def test_custom_metadata(self) -> None:
        """Test custom metadata storage"""
        metadata = WorkItemMetadata()
        metadata.custom_metadata["key"] = "value"
        assert metadata.custom_metadata["key"] == "value"

    def test_get_stage_duration(self) -> None:
        """Test stage duration calculation"""
        metadata = WorkItemMetadata()

        # Add started transition
        metadata.add_stage_transition("processing", "started")

        # Add completed transition (small delay will occur naturally)
        metadata.add_stage_transition("processing", "completed")

        duration = metadata.get_stage_duration("processing")
        assert duration is not None
        assert duration >= 0

        # Test non-existent stage
        assert metadata.get_stage_duration("nonexistent") is None

        # Test incomplete stage (only started, not completed)
        metadata.add_stage_transition("incomplete", "started")
        assert metadata.get_stage_duration("incomplete") is None


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
        metadata.completed = metadata.created + wait_time

        result = PipelineResult(job_id="test", success=True, metadata=metadata)
        assert result.total_duration == wait_time.total_seconds()

    def test_total_duration_incomplete(self) -> None:
        """Test duration is None for incomplete items"""
        result = PipelineResult(job_id="test", success=False, metadata=WorkItemMetadata())
        assert result.total_duration is None
