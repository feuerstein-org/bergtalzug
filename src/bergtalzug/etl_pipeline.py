"""
ETL Pipeline Implementation

This module implements an ETL (Extract, Transform, Load) pipeline using asyncio and culsans for concurrency.
By default processing is done via async (`process` function), however you can use the `sync_process` method
for synchronous processing with threads. This will use the ThreadPoolExecutor instead.

As of now if `process` is defined the code will run in the same thread as async and
if `sync_process` is defined it will run in a separate thread.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
from typing import Any, Annotated
from collections.abc import Callable
import culsans
from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime, timezone
from uuid import uuid4
import contextlib


class PipelineStage(Enum):
    """Enum for pipeline stages"""

    CREATED = "created"
    QUEUED_FETCH = "queued_fetch"
    FETCHING = "fetching"
    QUEUED_PROCESS = "queued_process"
    PROCESSING = "processing"
    QUEUED_STORE = "queued_store"
    STORING = "storing"
    COMPLETED = "completed"
    ERROR = "error"


@dataclass
class WorkItemMetadata:
    """Structured metadata for WorkItem lifecycle tracking"""

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    stage: PipelineStage = PipelineStage.CREATED
    fetch_started_at: datetime | None = None
    fetch_completed_at: datetime | None = None
    process_started_at: datetime | None = None
    process_completed_at: datetime | None = None
    store_started_at: datetime | None = None
    store_completed_at: datetime | None = None
    completed_at: datetime | None = None
    error_details: dict[str, Any] | None = None
    custom_metadata: dict[str, Any] = field(default_factory=lambda: {})

    def add_stage_transition(self, stage: PipelineStage) -> None:
        """Record a stage transition with timestamp"""
        self.stage = stage


@dataclass
class WorkItem:
    """
    Represents a work item in the ETL pipeline.

    It contains the data to be processed, metadata about the item, and a unique job ID.
    Objects of this class will be passed through all the stages of the ETL pipeline.
    """

    data: bytes
    metadata: WorkItemMetadata = field(default_factory=WorkItemMetadata)
    job_id: str = field(default_factory=lambda: str(uuid4()))


@dataclass
class PipelineResult:
    """Result of a completed WorkItem"""

    job_id: str
    success: bool
    metadata: WorkItemMetadata
    error: Exception | None = None

    @property
    def total_duration(self) -> float | None:
        """Total time from creation to completion"""
        if self.metadata.completed_at:
            return (self.metadata.completed_at - self.metadata.created_at).total_seconds()
        return None


# TODO: Currently both, read and write operation acquire a lock - this might be a bottleneck
# Potentially switch to a different approach like a queue, external database, Reader-writer locks etc.
class ItemTracker:
    """Tracks all items flowing through the pipeline"""

    def __init__(self) -> None:
        """Initialize the item tracker"""
        self._items: dict[str, WorkItem] = {}
        self._completed: dict[str, PipelineResult] = {}
        self._lock = asyncio.Lock()
        self._callbacks: list[Callable[[PipelineResult], None]] = []

    async def register_item(self, item: WorkItem) -> None:
        """Register a new item entering the pipeline"""
        async with self._lock:
            self._items[item.job_id] = item
            item.metadata.add_stage_transition(PipelineStage.CREATED)

    async def update_item_stage(self, job_id: str, stage: PipelineStage) -> None:
        """Update the stage of an item"""
        async with self._lock:
            if job_id in self._items:
                self._items[job_id].metadata.add_stage_transition(stage)

    async def complete_item(
        self, job_id: str, success: bool = True, error: Exception | None = None
    ) -> PipelineResult | None:
        """Mark an item as completed"""
        result = None
        callbacks_to_run = []

        async with self._lock:
            if job_id in self._items:
                item = self._items[job_id]
                item.metadata.completed_at = datetime.now(timezone.utc)
                item.metadata.add_stage_transition(PipelineStage.COMPLETED if success else PipelineStage.ERROR)

                result = PipelineResult(job_id=job_id, success=success, metadata=item.metadata, error=error)

                self._completed[job_id] = result
                del self._items[job_id]

                # Copy callbacks while still under lock
                callbacks_to_run = self._callbacks.copy()

        # Run callbacks outside the lock
        if result is not None:
            for callback in callbacks_to_run:
                try:
                    callback(result)
                except Exception as e:
                    logging.error("Callback error: %s", e)

        return result

    def add_completion_callback(self, callback: Callable[[PipelineResult], None]) -> None:
        """Add a callback to be called when items complete"""
        self._callbacks.append(callback)

    async def get_active_items(self) -> list[WorkItem]:
        """Get all currently active items"""
        async with self._lock:
            return list(self._items.values())

    async def get_completed_results(self) -> list[PipelineResult]:
        """Get all completed results"""
        async with self._lock:
            return list(self._completed.values())

    async def get_statistics(self) -> dict[str, Any]:
        """Get pipeline statistics"""
        async with self._lock:
            active_stages: dict[str, int] = {}
            for item in self._items.values():
                stage = item.metadata.stage.value
                active_stages[stage] = active_stages.get(stage, 0) + 1

            completed_count = len(self._completed)
            success_count = sum(1 for r in self._completed.values() if r.success)

            avg_duration = None
            if self._completed:
                durations = [r.total_duration for r in self._completed.values() if r.total_duration]
                if durations:
                    avg_duration = sum(durations) / len(durations)

            # TODO: This is potentially expensive, consider caching or optimizing
            # Stage-specific average durations
            stage_durations: dict[str, list[float]] = {"fetch": [], "process": [], "store": []}

            for result in self._completed.values():
                metadata = result.metadata

                # Fetch stage duration
                if metadata.fetch_started_at and metadata.fetch_completed_at:
                    fetch_duration = (metadata.fetch_completed_at - metadata.fetch_started_at).total_seconds()
                    stage_durations["fetch"].append(fetch_duration)

                # Process stage duration
                if metadata.process_started_at and metadata.process_completed_at:
                    process_duration = (metadata.process_completed_at - metadata.process_started_at).total_seconds()
                    stage_durations["process"].append(process_duration)

                # Store stage duration
                if metadata.store_started_at and metadata.store_completed_at:
                    store_duration = (metadata.store_completed_at - metadata.store_started_at).total_seconds()
                    stage_durations["store"].append(store_duration)

            # Averages for each stage
            avg_stage_durations = {}
            for stage, durations in stage_durations.items():
                if durations:
                    avg_stage_durations[f"average_{stage}_duration_seconds"] = sum(durations) / len(durations)
                else:
                    avg_stage_durations[f"average_{stage}_duration_seconds"] = None

            return {
                "active_items": len(self._items),
                "active_by_stage": active_stages,
                "completed_items": completed_count,
                "success_rate": success_count / completed_count if completed_count > 0 else 0,
                "average_duration_seconds": avg_duration,
                **avg_stage_durations,
            }

    async def log_statistics(self, logger: logging.Logger) -> None:
        """Log pipeline statistics"""
        stats = await self.get_statistics()
        logger.info(
            "Pipeline Stats | Active: %d | Active items by stage: %s | Completed: %d | Success Rate: %.2f%% | Avg Duration: %.2fs | Avg Fetch Duration: %.2fs | Avg Process Duration: %.2fs | Avg Store Duration: %.2fs",
            stats["active_items"],
            stats["active_by_stage"],
            stats["completed_items"],
            stats["success_rate"] * 100,
            stats["average_duration_seconds"] or 0,
            stats["average_fetch_duration_seconds"] or 0,
            stats["average_process_duration_seconds"] or 0,
            stats["average_store_duration_seconds"] or 0,
        )


PositiveInt = Annotated[int, Field(gt=0, strict=True)]
NonNegativeFloat = Annotated[float, Field(ge=0, strict=True)]


# TODO: each workers should have an ID for identification/logging
class ETLPipelineConfig(BaseModel):
    """
    Configuration model for ETL Pipeline with validation.

    Args:
        pipeline_name (str): Name of the pipeline for logging.
        fetch_workers (PositiveInt): Number of concurrent fetch workers.
        process_workers (PositiveInt): Number of processing workers.
        store_workers (PositiveInt): Number of store workers.
        fetch_queue_size (PositiveInt): Size of the fetch queue.
        process_queue_size (PositiveInt): Size of the process queue.
        store_queue_size (PositiveInt): Size of the store queue.
        queue_refresh_rate (NonNegativeFloat): Interval in seconds to check and refill the fetch queue.
        enable_tracking (bool): Whether to enable item tracking, will return PipelineResult on completion.
        stats_interval_seconds (NonNegativeFloat): Interval in seconds to report pipeline statistics.

    Raises:
        ValidationError: If any of the provided parameters are invalid

    """

    pipeline_name: str = "etl_pipeline"
    fetch_workers: PositiveInt = 10
    process_workers: PositiveInt = 5
    store_workers: PositiveInt = 10
    fetch_queue_size: PositiveInt = 1000
    process_queue_size: PositiveInt = 500
    store_queue_size: PositiveInt = 1000
    queue_refresh_rate: NonNegativeFloat = 1.0  # seconds
    stats_interval_seconds: NonNegativeFloat = 10.0
    enable_tracking: bool = Field(default=True, strict=True)

    # TODO: Potentially add some validation functions, e.g. too big/small queue etc.


# TODO: Make some properties public and modifiable, e.g. queue refresh rate or fetch workers
# if dynamic worker size is available
class ETLPipeline(ABC):
    """
    Abstract base class for an ETL pipeline.

    Data will be passed through the stages of the pipeline as `WorkItem` objects.
    All steps will be executed concurrently and asynchronously. Just override the abstract methods.

    The specific methods are:
    refill_queue: which is called periodically to add items to the queue if it falls below a threshold.
    fetch: which is called to fetch data.
    process: async function which is called to process the fetched data.
    sync_process: sync function that is called to process the fetched data in a thread.
    store: which is called to store the processed data.

    Note: only process or sync_process can be defined, not both.
    Only use sync_process if you have non-async compatible code.

    Setting enable_tracking in the ETLPipelineConfig is required to track items as well as
    get PipelineResult on completion of processing.
    """

    def __init__(self, config: ETLPipelineConfig | None = None, **kwargs: Any) -> None:
        """
        Initialize the ETL pipeline with validated configuration.

        Args:
            config: ETLPipelineConfig object with validated parameters
            **kwargs: Individual parameters (will be validated and converted to config)

        Raises:
            ValidationError: If any of the provided parameters are invalid

        """
        if config is None:
            config = ETLPipelineConfig(**kwargs)
        elif kwargs:
            # If both config and kwargs are provided, raise an error to avoid confusion
            raise ValueError("Cannot provide both 'config' and individual parameters via kwargs")

        self.config = config
        self.logger = logging.getLogger(f"etl.{config.pipeline_name}")
        self.pipeline_name = config.pipeline_name
        self._fetch_workers = config.fetch_workers
        self._process_workers = config.process_workers
        self._store_workers = config.store_workers
        self._fetch_queue_size = config.fetch_queue_size
        self._process_queue_size = config.process_queue_size
        self._store_queue_size = config.store_queue_size
        self._process_executor = None
        self._process_is_sync = self._check_process_is_sync()
        if self._process_is_sync:
            self._process_executor = ThreadPoolExecutor(max_workers=self._process_workers)

        self._queue_refresh_rate = config.queue_refresh_rate

        # Initialize tracker
        self.tracker = ItemTracker() if config.enable_tracking else None
        # Statistics task
        self._stats_task: asyncio.Task[None] | None = None

    def _check_process_is_sync(self) -> bool:
        """Validates that exactly one of process or sync_process is implemented."""
        # Check which methods are implemented
        has_async = "process" in self.__class__.__dict__
        has_sync = "sync_process" in self.__class__.__dict__

        if has_sync and has_async:
            raise TypeError(f"Class {self.__class__.__name__} cannot implement both process and sync_process")
        if not has_sync and not has_async:
            raise TypeError(f"Class {self.__class__.__name__} must implement either process or sync_process")

        return has_sync

    @abstractmethod
    async def refill_queue(self, count: int) -> list[WorkItem]:
        """
        Abstract method that gets called periodically to add items to the queue if falls below a threshold.

        A list of `count` WorkItems should be returned which will be passed to the fetch function one by one.
        e.g. if count is 10, then 10 items should be returned.
        If an Empty list is returned this signifies that the ETL job is finished.
        """
        pass

    @abstractmethod
    async def fetch(self, item: WorkItem) -> WorkItem:
        """
        Abstract method that gets called when an item from the queue needs to be fetched.

        After the item is fetched it should be simply returned as a WorkItem.
        """
        pass

    async def process(self, item: WorkItem) -> WorkItem:
        """
        Abstract method that gets called when an item was passed from the fetch method.

        Note that this method is async.
        Use the `sync_process` method if you have non-async workload.
        This will run the code in a separate thread.
        Only one of the two can be defined.

        Once processing is done the item should be returned as a WorkItem.
        """
        raise NotImplementedError(
            "The async process method should be implemented in a subclass."
            "Either 'process' or 'sync_process' must be implemented, but not both."
        )

    # TODO: Add a way to switch between ThreadPoolExecutor and ProcessPoolExecutor
    # This is hard because passing the function to the ProcessPoolExecutor requires serialization
    # of the entire class unless we create a static method just for this.
    # Serializing the whole class is very inefficient, even the new InterpreterPoolExecutor
    # requires this type of serialiazation which is not ideal.
    # With the static method approach, this would mean we would have,
    # `process`, `sync_process` and `static_sync_process` functions.
    # Will need to see if there is a better way. Also at some stage we want to have a way
    # to "build the pipeline" in any way we want, e.g. no hardcoded stages.
    # It's best to add this feature once dynamic stages are being implemented.
    def sync_process(self, item: WorkItem) -> WorkItem:
        """
        Abstract method that gets called when an item was passed from the fetch method.

        Note that this method is a sync method for multithreading workloads.
        This method should only be used if execution is needed to be done in a separate thread.

        If asyncio can be used, use the `process` method instead.
        Only one of the two can be defined.

        Once processing is done the item should be returned as a WorkItem.
        """
        raise NotImplementedError(
            "The sync process method should be implemented in a subclass."
            "Either 'process' or 'sync_process' must be implemented, but not both."
        )

    @abstractmethod
    async def store(self, item: WorkItem) -> None:
        """
        Abstract method that gets called when an item was processed.

        This method should handle the storage of the processed data.
        It is expected to be asynchronous, e.g., storeing to S3.
        """
        pass

    @property
    def fetch_workers(self) -> int:
        """Number of fetch workers"""
        return self._fetch_workers

    @property
    def process_workers(self) -> int:
        """Number of process workers (async or sync)"""
        return self._process_workers

    @property
    def store_workers(self) -> int:
        """Number of store workers"""
        return self._store_workers

    @property
    def fetch_queue_size(self) -> int:
        """Size of the fetch queue"""
        return self._fetch_queue_size

    @property
    def process_queue_size(self) -> int:
        """Size of the process queue"""
        return self._process_queue_size

    @property
    def store_queue_size(self) -> int:
        """Size of the store queue"""
        return self._store_queue_size

    @property
    def process_is_sync(self) -> bool:
        """Whether the process stage is synchronous."""
        return self._process_is_sync

    @property
    def queue_refresh_rate(self) -> float:
        """Rate at which the queue is refreshed."""
        return self._queue_refresh_rate

    async def _setup_queues(self) -> None:
        """
        Initialize queues.

        This method sets up the queues used in the ETL pipeline.
        """
        # Job queue for initial distribution
        self._fetch_queue = culsans.Queue[WorkItem | None](maxsize=self._fetch_queue_size)

        # culsans queues provide both async and sync interfaces
        self._process_queue = culsans.Queue[WorkItem | None](maxsize=self._process_queue_size)
        self._store_queue = culsans.Queue[WorkItem | None](maxsize=self._store_queue_size)

    async def _periodic_stats_reporter(self) -> None:
        """Periodically report pipeline statistics"""
        while True:
            try:
                if self.tracker:
                    await self.tracker.log_statistics(self.logger)
            except Exception as e:
                self.logger.error("Error reporting stats: %s", e)

            await asyncio.sleep(self.config.stats_interval_seconds)

    def add_completion_callback(self, callback: Callable[[PipelineResult], None]) -> None:
        """Add a callback to be notified when items complete"""
        if self.tracker:
            self.tracker.add_completion_callback(callback)
        else:
            self.logger.warning("Item tracking is disabled, cannot add completion callback")

    async def get_active_items(self) -> list[WorkItem]:
        """Get currently active items in the pipeline"""
        if self.tracker:
            return await self.tracker.get_active_items()

        self.logger.warning("Item tracking is disabled, cannot get active items")
        return []

    async def get_pipeline_stats(self) -> dict[str, Any]:
        """Get current pipeline statistics"""
        if self.tracker:
            return await self.tracker.get_statistics()

        self.logger.warning("Item tracking is disabled, cannot get statistics")
        return {}

    async def _periodic_queue_manager(self) -> None:
        """Periodically check queue size and add items if below 50% capacity."""
        while True:
            current_size = self._fetch_queue.qsize()
            threshold = self._fetch_queue_size / 2  # 50% threshold, TODO: make configurable

            if current_size < threshold:
                self.logger.debug(
                    "Fetch queue size below threshold (%s/%s), adding items", current_size, self._fetch_queue_size
                )
                target_size = int(self._fetch_queue_size * 0.9)
                available_space = target_size - current_size
                # Request 90% of available space as safety buffer, later might need to add buffer due to overflow risk
                count = max(1, available_space)  # even if available space is 0, we want to refill at least one item
                try:
                    items = await self.refill_queue(count)

                    if not items:
                        self.logger.info("No more items available, initiating shutdown")
                        break

                    # Add all items to the fetch queue
                    for item in items:
                        if self.tracker:
                            await self.tracker.register_item(item)
                            await self.tracker.update_item_stage(item.job_id, PipelineStage.QUEUED_FETCH)
                        await self._fetch_queue.async_put(item)

                    self.logger.debug("Added %s items to fetch queue", len(items))

                except Exception as e:
                    self.logger.error("Error in refill_queue: %s", e)
                    # Continue running - don't break the pipeline for this error
            # Check every second
            await asyncio.sleep(self._queue_refresh_rate)

    async def _fetch_worker(self) -> None:
        """
        Async fetching worker.

        Gets fetch jobs from the fetch queue and gets data asynchronously.
        """
        while True:
            work_item = await self._fetch_queue.async_get()
            if work_item is None:  # Poison pill
                self.logger.info("Fetch worker received poison pill, shutting down")
                break

            try:
                if self.tracker:
                    await self.tracker.update_item_stage(work_item.job_id, PipelineStage.FETCHING)
                work_item.metadata.fetch_started_at = datetime.now(timezone.utc)
                self.logger.debug(
                    "Received fetch job from queue, calling abstract fetch function for job %s", work_item.job_id
                )

                fetched_item = await self.fetch(work_item)

                fetched_item.metadata.fetch_completed_at = datetime.now(timezone.utc)
                if self.tracker:
                    await self.tracker.update_item_stage(fetched_item.job_id, PipelineStage.QUEUED_PROCESS)
                await self._process_queue.async_put(fetched_item)
            except Exception as e:
                self.logger.error("Fetch error for job %s: %s", work_item.job_id, e)
                if self.tracker:
                    await self.tracker.complete_item(work_item.job_id, success=False, error=e)
                # TODO: Add retry logic (DLQ) or error handling or dead-letter queue + log
            finally:
                # For now still need to mark as done even on failure to prevent deadlock
                # This WILL loose items in case of error!!
                self._fetch_queue.task_done()

    async def _process_worker(self) -> None:
        """
        Async process worker.

        Gets process jobs from the process queue and processes them.
        """
        while True:
            work_item = await self._process_queue.async_get()
            if work_item is None:  # Poison pill
                self.logger.info("Process worker received poison pill, shutting down")
                break

            try:
                if self.tracker:
                    await self.tracker.update_item_stage(work_item.job_id, PipelineStage.PROCESSING)
                work_item.metadata.process_started_at = datetime.now(timezone.utc)
                self.logger.debug(
                    "Received process job from queue, calling abstract process function for job %s", work_item.job_id
                )

                # pick execution strategy
                if not self._process_is_sync:
                    processed_item = await self.process(work_item)
                else:
                    loop = asyncio.get_running_loop()
                    processed_item = await loop.run_in_executor(self._process_executor, self.sync_process, work_item)

                processed_item.metadata.process_completed_at = datetime.now(timezone.utc)
                if self.tracker:
                    await self.tracker.update_item_stage(processed_item.job_id, PipelineStage.QUEUED_STORE)
                await self._store_queue.async_put(processed_item)
            except Exception as e:
                self.logger.error("Process error for job %s: %s", work_item.job_id, e)
                if self.tracker:
                    await self.tracker.complete_item(work_item.job_id, success=False, error=e)
                # TODO: Add retry logic (DLQ) or error handling or dead-letter queue + log
            finally:
                # For now still need to mark as done even on failure to prevent deadlock
                # This WILL loose items in case of error!!
                self._process_queue.task_done()
        return

    async def _store_worker(self) -> None:
        """
        Async storing worker.

        Gets store jobs from the store queue and stores data asynchronously.
        """
        while True:
            work_item = await self._store_queue.async_get()
            if work_item is None:  # Poison pill
                self.logger.info("Store worker received poison pill, shutting down")
                break

            try:
                if self.tracker:
                    await self.tracker.update_item_stage(work_item.job_id, PipelineStage.STORING)
                work_item.metadata.store_started_at = datetime.now(timezone.utc)
                self.logger.debug(
                    "Received store job from queue, calling abstract store function for job %s", work_item.job_id
                )

                await self.store(work_item)

                work_item.metadata.store_completed_at = datetime.now(timezone.utc)
                if self.tracker:
                    # Mark as completed
                    await self.tracker.complete_item(work_item.job_id, success=True)
            except Exception as e:
                self.logger.error("Storing error for job %s: %s", work_item.job_id, e)
                if self.tracker:
                    await self.tracker.complete_item(work_item.job_id, success=False, error=e)
                # TODO: Add retry logic or error handling or dead-letter queue + log
            finally:
                # For now still need to mark as done even on failure to prevent deadlock
                # This WILL loose items in case of error!!
                self._store_queue.task_done()

    # NOTE: Potentially allow to pass initial jobs to the pipeline
    # Also might be worth adding a setup abstract method to initialize the pipeline
    async def run(self) -> list[PipelineResult]:
        """
        Start the ETL pipeline.

        This method sets up the queues, starts the workers, and manages the lifecycle of the ETL process.
        """
        self.logger.info("Initializing ETL pipeline: %s", self.pipeline_name)

        # Setup queues
        await self._setup_queues()
        # Start periodic queue manager
        queue_manager_task = asyncio.create_task(self._periodic_queue_manager())

        # Start statistics reporter if enabled
        if self.config.enable_tracking and self.config.stats_interval_seconds > 0:
            self._stats_task = asyncio.create_task(self._periodic_stats_reporter())

        # Start download workers
        fetch_tasks: list[asyncio.Task[None]] = []
        for _ in range(self._fetch_workers):
            task = asyncio.create_task(self._fetch_worker())
            fetch_tasks.append(task)

        # Start process workers
        process_tasks: list[asyncio.Task[None]] = []
        for _ in range(self._process_workers):
            task = asyncio.create_task(self._process_worker())
            process_tasks.append(task)

        # Start store workers
        store_tasks: list[asyncio.Task[None]] = []
        for _ in range(self._store_workers):
            task = asyncio.create_task(self._store_worker())
            store_tasks.append(task)

        self.logger.info(
            "ETL pipeline %s started | fetch_workers: %s (queue: %s) "
            "| process_workers: %s (queue: %s) | store_workers: %s (queue: %s)",
            self.pipeline_name,
            self._fetch_workers,
            self._fetch_queue_size,
            self._process_workers,
            self._process_queue_size,
            self._store_workers,
            self._store_queue_size,
        )

        # Wait for the queue manager to signal completion
        await queue_manager_task

        self.logger.info("Waiting for all jobs to complete")

        # Sequential shutdown
        await self._fetch_queue.async_join()
        self.logger.info("All fetch jobs completed, shutting down fetch workers")
        for _ in range(self._fetch_workers):
            await self._fetch_queue.async_put(None)

        await self._process_queue.async_join()
        self.logger.info("All process jobs completed, shutting down process workers")
        for _ in range(self._process_workers):
            await self._process_queue.async_put(None)

        await self._store_queue.async_join()
        self.logger.info("All store jobs completed, shutting down store workers")
        for _ in range(self._store_workers):
            await self._store_queue.async_put(None)

        # Wait for workers to finish
        await asyncio.gather(*fetch_tasks)
        await asyncio.gather(*process_tasks)
        await asyncio.gather(*store_tasks)

        # Stop stats reporter
        if self._stats_task:
            self._stats_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stats_task

        # Close queues, TODO: Is this needed?
        self._fetch_queue.close()
        self._process_queue.close()
        self._store_queue.close()
        await self._fetch_queue.wait_closed()
        await self._process_queue.wait_closed()
        await self._store_queue.wait_closed()

        # Get final results
        results = []
        if self.tracker:
            results = await self.tracker.get_completed_results()
            await self.tracker.log_statistics(self.logger)
        else:
            self.logger.info("ETL pipeline %s completed", self.pipeline_name)

        # Shutdown executor
        return results


# if __name__ == "__main__":
#     print("ETL pipeline started")
#     import time
#     time.sleep(10000)
