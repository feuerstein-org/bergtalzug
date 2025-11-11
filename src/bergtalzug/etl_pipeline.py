"""
Dynamic ETL Pipeline Implementation

This module implements a flexible ETL pipeline with user-defined stages using asyncio and culsans.
Users can configure any number of stages, each with custom concurrency models:
- Async stages (native asyncio)
- Sync stages with ThreadPoolExecutor
- Sync stages with ProcessPoolExecutor
- Sync stages with InterpreterPoolExecutor (Python 3.14+)

The pipeline handles queueing, tracking, statistics, and lifecycle management automatically.
"""

import asyncio
import logging
import inspect
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, InterpreterPoolExecutor
from abc import abstractmethod
from typing import Any, Annotated, cast
from collections.abc import Callable, Awaitable
import culsans
from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime, timezone
from uuid import uuid4
import contextlib


@dataclass
class WorkItemMetadata:
    """Structured metadata for WorkItem lifecycle tracking with dynamic stages"""

    current_stage: str = "created"
    created: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed: datetime | None = None
    error_details: dict[str, Any] | None = None
    custom_metadata: dict[str, Any] = field(default_factory=lambda: {})
    # Dynamic stage timing: {stage_name: {"queued": datetime, "started": datetime, "completed": datetime}}
    stage_timings: dict[str, dict[str, datetime]] = field(default_factory=lambda: {})

    def add_stage_transition(self, stage: str, transition_type: str = "started") -> None:
        """
        Record a stage transition with timestamp.

        Args:
            stage: Name of the stage
            transition_type: Type of transition - "queued", "started", or "completed"

        """
        self.current_stage = stage
        if stage not in self.stage_timings:
            self.stage_timings[stage] = {}

        timestamp_key = f"{transition_type}"
        self.stage_timings[stage][timestamp_key] = datetime.now(timezone.utc)

    def get_stage_duration(self, stage: str) -> float | None:
        """Get the duration of a stage in seconds"""
        if stage not in self.stage_timings:
            return None

        timings = self.stage_timings[stage]
        started = timings.get("started")
        completed = timings.get("completed")

        if started and completed:
            return (completed - started).total_seconds()
        return None


@dataclass
class WorkItem:
    """
    Represents a work item in the ETL pipeline.

    It contains the data to be processed, metadata about the item, and a unique job ID.
    Objects of this class will be passed through all the stages of the ETL pipeline.
    """

    data: Any
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
        if self.metadata.completed:
            return (self.metadata.completed - self.metadata.created).total_seconds()
        return None


# TODO: Currently both, read and write operation acquire a lock - this might be a bottleneck
# Potentially switch to a different approach like a queue, Reader-writer locks etc.
class ItemTracker:
    """Tracks all items flowing through the pipeline with dynamic stages"""

    def __init__(self, stage_names: list[str]) -> None:
        """
        Initialize the item tracker.

        Args:
            stage_names: List of stage names in the pipeline

        """
        self._items: dict[str, WorkItem] = {}
        self._completed: dict[str, PipelineResult] = {}
        self._lock = asyncio.Lock()
        self._callbacks: list[Callable[[PipelineResult], None]] = []
        self._stage_names = stage_names

    async def register_item(self, item: WorkItem) -> None:
        """Register a new item entering the pipeline"""
        async with self._lock:
            self._items[item.job_id] = item
            item.metadata.add_stage_transition("created")

    async def update_item_stage(self, job_id: str, stage: str, transition_type: str = "started") -> None:
        """
        Update the stage of an item.

        Args:
            job_id: The job ID
            stage: The stage name
            transition_type: Type of transition - "queued", "started", or "completed"

        """
        async with self._lock:
            if job_id in self._items:
                self._items[job_id].metadata.add_stage_transition(stage, transition_type)

    async def complete_item(
        self, job_id: str, success: bool = True, error: Exception | None = None
    ) -> PipelineResult | None:
        """Mark an item as completed"""
        callbacks_to_run = []

        async with self._lock:
            if job_id not in self._items:
                # TODO: Maybe refactor the code to also check for correct IDs after each stage
                # This check only applies for the refill_queue function currently
                raise RuntimeError(
                    f"Unknown job ID: {job_id} - this could be due to `refill_queue` items with duplicate job IDs"
                )

            item = self._items[job_id]
            item.metadata.completed = datetime.now(timezone.utc)
            item.metadata.current_stage = "completed" if success else "error"

            result = PipelineResult(job_id=job_id, success=success, metadata=item.metadata, error=error)

            self._completed[job_id] = result
            del self._items[job_id]

            # Copy callbacks while still under lock
            callbacks_to_run = self._callbacks.copy()

        # Run callbacks outside the lock
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
                stage = item.metadata.current_stage
                active_stages[stage] = active_stages.get(stage, 0) + 1

            completed_count = len(self._completed)
            success_count = sum(1 for r in self._completed.values() if r.success)

            avg_duration = None
            if self._completed:
                durations = [r.total_duration for r in self._completed.values() if r.total_duration]
                if durations:
                    avg_duration = sum(durations) / len(durations)

            # This is potentially expensive, consider caching or optimizing
            # Stage-specific average durations
            stage_durations: dict[str, list[float]] = {stage: [] for stage in self._stage_names}

            for result in self._completed.values():
                metadata = result.metadata
                for stage in self._stage_names:
                    duration = metadata.get_stage_duration(stage)
                    if duration is not None:
                        stage_durations[stage].append(duration)

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

        # Build stage duration string
        stage_durations_str = " | ".join(
            [
                f"Avg {stage} Duration: {stats.get(f'average_{stage}_duration_seconds', 0) or 0:.2f}s"
                for stage in self._stage_names
            ]
        )

        logger.info(
            "Pipeline Stats | Active: %d | Active items by stage: %s | Completed: %d | Success Rate: %.2f%% | Avg Duration: %.2fs | %s",
            stats["active_items"],
            stats["active_by_stage"],
            stats["completed_items"],
            stats["success_rate"] * 100,
            stats["average_duration_seconds"] or 0,
            stage_durations_str,
        )


PositiveInt = Annotated[int, Field(gt=0, strict=True)]
NonNegativeFloat = Annotated[float, Field(ge=0, strict=True)]


class ExecutionType(str, Enum):
    """Execution types for pipeline stages"""

    ASYNC = "async"
    THREAD = "thread"
    PROCESS = "process"
    INTERPRETER = "interpreter"


class StageConfig(BaseModel):
    """
    Configuration for a single pipeline stage.

    Args:
        name: Unique name for the stage
        execution_type: How to execute the stage (async/thread/process/interpreter)
        workers: Number of concurrent workers for this stage
        queue_size: Maximum size of the queue feeding this stage

    """

    name: str
    execution_type: ExecutionType = ExecutionType.ASYNC
    workers: PositiveInt = 5
    queue_size: PositiveInt = 1000


class ETLPipelineConfig(BaseModel):
    """
    Configuration model for dynamic ETL Pipeline with validation.

    Args:
        pipeline_name: Name of the pipeline for logging
        stages: List of stage configurations defining the pipeline
        queue_refresh_rate: Interval in seconds to check and refill the first queue
        enable_tracking: Whether to enable item tracking, will return PipelineResult on completion
        stats_interval_seconds: Interval in seconds to report pipeline statistics - if 0 disables reporting

    Raises:
        ValidationError: If any of the provided parameters are invalid

    """

    pipeline_name: str = "etl_pipeline"
    stages: list[StageConfig] = Field(min_length=1)
    queue_refresh_rate: NonNegativeFloat = 1.0  # seconds
    stats_interval_seconds: NonNegativeFloat = 10.0
    enable_tracking: bool = Field(default=True, strict=True)

    def model_post_init(self, __context: Any) -> None:
        """Validate pipeline configuration"""
        # Check for duplicate stage names
        stage_names = [stage.name for stage in self.stages]
        if len(stage_names) != len(set(stage_names)):
            raise ValueError("Stage names must be unique")


# Type alias for stage handlers
StageHandler = (
    Callable[[WorkItem], Awaitable[WorkItem]] | Callable[[WorkItem], WorkItem] | Callable[[str, Any], WorkItem]
)


class ETLPipeline:
    """
    Abstract base class for a dynamic ETL pipeline.

    Data flows through user-defined stages as `WorkItem` objects.
    Each stage can be either async or sync, with sync stages supporting
    ThreadPoolExecutor, ProcessPoolExecutor, or InterpreterPoolExecutor.

    Users must:
    1. Define a config with their desired stages (StageConfig)
    2. Implement refill_queue() to provide work items
    3. Provide handler functions for each stage via register_stage_handler()

    The pipeline handles all concurrency, queueing, tracking, and lifecycle management.
    """

    def __init__(self, config: ETLPipelineConfig) -> None:
        """
        Initialize the dynamic ETL pipeline.

        Args:
            config: ETLPipelineConfig with stages and settings

        Raises:
            ValueError: If configuration is invalid

        """
        self.config = config
        self.logger = logging.getLogger(f"etl.{config.pipeline_name}")
        self.pipeline_name = config.pipeline_name
        self._queue_refresh_rate = config.queue_refresh_rate

        # Stage setup
        self._stages = config.stages
        self._stage_handlers: dict[str, StageHandler] = {}
        self._queues: dict[str, culsans.Queue[WorkItem | None]] = {}
        self._executors: dict[str, ThreadPoolExecutor | ProcessPoolExecutor | InterpreterPoolExecutor] = {}

        # Initialize tracker with stage names
        stage_names = [stage.name for stage in self._stages]
        self.tracker = ItemTracker(stage_names) if config.enable_tracking else None

        # Statistics task
        self._stats_task: asyncio.Task[None] | None = None

        # Pipeline lifecycle tracking
        self._running = False
        self._queue_manager_task: asyncio.Task[None] | None = None
        self._worker_tasks: list[list[asyncio.Task[None]]] = []

    def register_stage_handler(self, stage_name: str, handler: StageHandler) -> None:
        """
        Register a handler function for a stage.

        Args:
            stage_name: Name of the stage (must match a StageConfig name)
            handler: The function to execute for this stage

        Raises:
            ValueError: If stage_name is not in the configured stages
            TypeError: If handler signature doesn't match execution type requirements

        """
        stage_config = next((s for s in self._stages if s.name == stage_name), None)
        if stage_config is None:
            raise ValueError(f"Stage '{stage_name}' not found in pipeline configuration")

        # Validate handler matches stage configuration
        self._validate_handler_signature(stage_name, stage_config, handler)

        self._stage_handlers[stage_name] = handler
        self.logger.debug("Registered handler for stage '%s'", stage_name)

    def _validate_handler_signature(self, stage_name: str, stage_config: StageConfig, handler: StageHandler) -> None:
        """
        Validate that handler function signature matches the execution type requirements.

        ASYNC/THREAD stages require: handler(item: WorkItem) -> WorkItem
        PROCESS/INTERPRETER stages require: handler(job_id: str, data: Any) -> Any

        Args:
            stage_name: Name of the stage (for error messages)
            stage_config: Stage configuration
            handler: The handler function to validate

        Raises:
            TypeError: If handler signature is invalid for the execution type

        """
        is_handler_async = inspect.iscoroutinefunction(handler)
        sig = inspect.signature(handler)
        params = list(sig.parameters.values())

        # Check async/sync consistency
        if stage_config.execution_type == ExecutionType.ASYNC and not is_handler_async:
            raise TypeError(
                f"Stage '{stage_name}' is configured with execution_type=ASYNC but handler is not async. "
                f"Expected: async def {handler.__name__}(item: WorkItem) -> WorkItem"
            )
        if stage_config.execution_type != ExecutionType.ASYNC and is_handler_async:
            raise TypeError(
                f"Stage '{stage_name}' is configured with execution_type={stage_config.execution_type.value} "
                f"but handler is async. Expected synchronous function."
            )

        # Check parameter signature based on execution type

        # Should have exactly 1 parameter (WorkItem)
        if stage_config.execution_type in (ExecutionType.ASYNC, ExecutionType.THREAD) and len(params) != 1:
            raise TypeError(
                f"Stage '{stage_name}' handler for {stage_config.execution_type.value} execution "
                f"must accept exactly 1 parameter (item: WorkItem), got {len(params)} parameters. "
                f"Expected signature: {'async ' if is_handler_async else ''}def {handler.__name__}(item: WorkItem) -> WorkItem"
            )

        # Should have exactly 2 parameters (job_id: str, data: Any)
        if stage_config.execution_type in (ExecutionType.PROCESS, ExecutionType.INTERPRETER) and len(params) != 2:  # noqa: PLR2004
            raise TypeError(
                f"Stage '{stage_name}' handler for {stage_config.execution_type.value} execution "
                f"must accept exactly 2 parameters (job_id: str, data: Any), got {len(params)} parameters. "
                f"Expected signature: def {handler.__name__}(job_id: str, data: Any) -> Any"
            )

    @abstractmethod
    async def refill_queue(self, count: int) -> list[WorkItem]:
        """
        Periodically called to add items to the first stage queue.

        Args:
            count: Number of items requested

        Returns:
            List of WorkItems to add to pipeline. Empty list signals completion.

        """
        pass

    async def _setup_queues(self) -> None:
        """Initialize queues for all stages"""
        for stage in self._stages:
            self._queues[stage.name] = culsans.Queue[WorkItem | None](maxsize=stage.queue_size)
            self.logger.debug("Created queue for stage '%s' with size %d", stage.name, stage.queue_size)

    async def _setup_executors(self) -> None:
        """Initialize executors for sync stages"""
        for stage in self._stages:
            if stage.execution_type == ExecutionType.THREAD:
                self._executors[stage.name] = ThreadPoolExecutor(max_workers=stage.workers)
            elif stage.execution_type == ExecutionType.PROCESS:
                self._executors[stage.name] = ProcessPoolExecutor(max_workers=stage.workers)
            elif stage.execution_type == ExecutionType.INTERPRETER:
                self._executors[stage.name] = InterpreterPoolExecutor(max_workers=stage.workers)

            if stage.execution_type != ExecutionType.ASYNC:
                self.logger.debug(
                    "Created %s executor for stage '%s' with %d workers",
                    stage.execution_type.value,
                    stage.name,
                    stage.workers,
                )

    async def _periodic_stats_reporter(self) -> None:
        """Periodically report pipeline statistics"""
        while True:
            await asyncio.sleep(self.config.stats_interval_seconds)
            try:
                if self.tracker:
                    await self.tracker.log_statistics(self.logger)
            except Exception as e:
                self.logger.error("Error reporting stats: %s", e)

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
        """Periodically check the first queue size and add items if below 50% capacity"""
        first_stage = self._stages[0]
        first_queue = self._queues[first_stage.name]
        threshold = first_stage.queue_size / 2  # 50% threshold

        while True:
            current_size = first_queue.qsize()

            if current_size < threshold:
                self.logger.debug(
                    "First queue '%s' size below threshold (%s/%s), adding items",
                    first_stage.name,
                    current_size,
                    first_stage.queue_size,
                )
                target_size = int(first_stage.queue_size * 0.9)
                available_space = target_size - current_size
                count = max(1, available_space)

                try:
                    items = await self.refill_queue(count)

                    if not items:
                        self.logger.info("No more items available, initiating shutdown")
                        break

                    # Add all items to the first queue
                    for item in items:
                        if self.tracker:
                            await self.tracker.register_item(item)
                            await self.tracker.update_item_stage(item.job_id, first_stage.name, "queued")
                        await first_queue.async_put(item)

                    self.logger.debug("Added %s items to first queue '%s'", len(items), first_stage.name)

                except Exception as e:
                    self.logger.error("Error in refill_queue: %s", e)
                    # Continue running - don't break the pipeline for this error

            await asyncio.sleep(self._queue_refresh_rate)

    async def _create_stage_worker(self, stage_idx: int) -> None:  # noqa C901
        """
        Generic worker for any stage.

        Args:
            stage_idx: Index of the stage in the stages list

        """
        stage = self._stages[stage_idx]
        stage_name = stage.name
        input_queue = self._queues[stage_name]

        # Determine output queue (next stage's queue or None for last stage)
        if stage_idx + 1 < len(self._stages):
            next_stage_name = self._stages[stage_idx + 1].name
            output_queue = self._queues[next_stage_name]
        else:
            output_queue = None
            next_stage_name = None

        handler: StageHandler = self._stage_handlers[stage_name]
        executor = self._executors.get(stage_name)
        loop = asyncio.get_running_loop()

        while True:
            work_item: WorkItem | None = await input_queue.async_get()
            if work_item is None:  # Poison pill
                self.logger.info("Stage '%s' worker received poison pill, shutting down", stage_name)
                break

            try:
                if self.tracker:
                    await self.tracker.update_item_stage(work_item.job_id, stage_name, "started")

                # Execute the handler
                if stage.execution_type == ExecutionType.ASYNC:
                    # Async handler
                    processed_item = cast(WorkItem, await handler(work_item))  # type: ignore
                elif stage.execution_type == ExecutionType.THREAD:
                    # Sync handler - run in thread
                    processed_item: WorkItem = await loop.run_in_executor(executor, handler, work_item)  # type: ignore
                else:
                    # Sync handler - run in executor
                    # Potentially, if needed later, pass in metadata as a dict or something similar
                    # since we can't pass as a class
                    data: Any = await loop.run_in_executor(executor, handler, work_item.job_id, work_item.data)  # type: ignore
                    work_item.data = data
                    processed_item = work_item

                if self.tracker:
                    await self.tracker.update_item_stage(work_item.job_id, stage_name, "completed")

                # Pass to next stage or complete
                if output_queue is not None:
                    next_stage_name = self._stages[stage_idx + 1].name
                    if self.tracker:
                        await self.tracker.update_item_stage(processed_item.job_id, next_stage_name, "queued")
                    await output_queue.async_put(processed_item)
                elif self.tracker:
                    # Last stage - mark as completed
                    await self.tracker.complete_item(work_item.job_id, success=True)

            except Exception as e:
                self.logger.error("Error in stage '%s' for job %s: %s", stage_name, work_item.job_id, e)
                if self.tracker:
                    await self.tracker.complete_item(work_item.job_id, success=False, error=e)

            finally:
                input_queue.task_done()

    @property
    def is_running(self) -> bool:
        """
        Check if the pipeline is currently running.

        Returns:
            True if pipeline is active, False otherwise

        """
        return self._running

    async def stop(self) -> None:
        """
        Gracefully stop the pipeline.

        This method cancels the queue manager task and initiates shutdown.
        Workers will finish processing current items before shutting down.

        Note: After calling stop(), you should still call run() to ensure
        all resources are properly cleaned up.
        """
        if not self._running:
            self.logger.warning("Pipeline is not running, nothing to stop")
            return

        self.logger.info("Stopping pipeline: %s", self.pipeline_name)

        if self._queue_manager_task and not self._queue_manager_task.done():
            self._queue_manager_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._queue_manager_task

        self.logger.info("Pipeline stop initiated. Call run() to complete shutdown.")

    async def start(self) -> None:
        """
        Start the ETL pipeline without blocking.

        This method initializes queues, executors, and workers, then returns immediately.
        Use `is_running()` to check status and `run()` to wait for completion.

        Example:
            ```python
            await pipeline.start()

            # Monitor while running
            while pipeline.is_running():
                stats = await pipeline.get_pipeline_stats()
                print(f"Active: {stats['active_items']}")
                await asyncio.sleep(1)

            # Wait for completion
            results = await pipeline.run()
            ```

        Raises:
            RuntimeError: If handlers are not registered

        """
        if self._running:
            self.logger.warning(
                "Pipeline is already running. Use stop() method to stop the pipeline before starting again."
            )
            return

        # Validate all stages have handlers
        for stage in self._stages:
            if stage.name not in self._stage_handlers:
                raise RuntimeError(
                    f"No handler registered for stage '{stage.name}'. Call register_stage_handler() first."
                )

        self.logger.info("Starting ETL pipeline: %s", self.pipeline_name)
        self._running = True

        # Setup queues and executors
        await self._setup_queues()
        await self._setup_executors()

        # Start periodic queue manager
        self._queue_manager_task = asyncio.create_task(self._periodic_queue_manager())

        # Start statistics reporter if enabled
        if self.config.enable_tracking and self.config.stats_interval_seconds > 0:
            self._stats_task = asyncio.create_task(self._periodic_stats_reporter())

        # Start workers for all stages
        self._worker_tasks = []

        for idx, stage in enumerate(self._stages):
            stage_tasks: list[asyncio.Task[None]] = []
            for _ in range(stage.workers):
                task = asyncio.create_task(self._create_stage_worker(idx))
                stage_tasks.append(task)
            self._worker_tasks.append(stage_tasks)
            self.logger.info(
                "Started %d workers for stage '%s' (queue size: %d)", stage.workers, stage.name, stage.queue_size
            )

        self.logger.info("Pipeline started successfully")

    async def run(self) -> list[PipelineResult]:
        """
        Start the ETL pipeline and block until completion.

        This method will call `start()` if the pipeline is not already running,
        then wait for all items to be processed and workers to shut down.

        For non-blocking operation with monitoring, use `start()` separately:
            ```python
            # Option 1: Simple blocking run
            results = await pipeline.run()

            # Option 2: Non-blocking with monitoring
            await pipeline.start()
            while pipeline.is_running():
                stats = await pipeline.get_pipeline_stats()
                print(f"Active: {stats['active_items']}")
                await asyncio.sleep(1)
            results = await pipeline.run()
            ```

        Returns:
            List of PipelineResults if tracking is enabled, empty list otherwise

        Raises:
            RuntimeError: If handlers are not registered

        """
        # Start pipeline if not already running
        if not self._running:
            await self.start()

        if self._queue_manager_task is None:
            raise RuntimeError("Pipeline not properly initialized")

        try:
            # Wait for the queue manager to signal completion
            await self._queue_manager_task

            self.logger.info("Waiting for all jobs to complete")

            # Sequential shutdown - join and poison pill each queue
            for idx, stage in enumerate(self._stages):
                queue = self._queues[stage.name]
                await queue.async_join()
                self.logger.info("All jobs for stage '%s' completed, shutting down workers", stage.name)

                # Send poison pills
                for _ in range(stage.workers):
                    await queue.async_put(None)

                # Wait for this stage's workers to finish
                await asyncio.gather(*self._worker_tasks[idx])

            # Stop stats reporter
            if self._stats_task:
                self._stats_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._stats_task

            # TODO: Is this necessary?
            # Close all queues
            for stage in self._stages:
                queue = self._queues[stage.name]
                queue.close()
                await queue.wait_closed()

            # Shutdown executors
            for executor in self._executors.values():
                executor.shutdown(wait=True)

            # Get final results
            results = []
            if self.tracker:
                results = await self.tracker.get_completed_results()
                if self.config.stats_interval_seconds > 0:
                    await self.tracker.log_statistics(self.logger)
            else:
                self.logger.info("ETL pipeline %s completed", self.pipeline_name)

            return results

        finally:
            self._running = False
            self._queue_manager_task = None
            self._worker_tasks = []
