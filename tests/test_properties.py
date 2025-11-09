"""Tests whether @property decorators are working as expected."""

import pytest
from conftest import MockETLPipelineFactory


@pytest.mark.asyncio
async def test_is_running_property(mock_etl_pipeline_factory: MockETLPipelineFactory) -> None:
    """Test the is_running property of the ETL pipeline"""
    pipeline = mock_etl_pipeline_factory.create()
    assert pipeline.is_running == False

    await pipeline.start()
    assert pipeline.is_running == True


# TODO: Add more properties in ETL pipeline and test them here
