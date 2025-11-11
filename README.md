# Bergtalzug

Bergtalzug is a flexible ETL (Extract, Transform, Load) framework that processes data concurrently and efficiently in Python. It utilizes multiple worker pools and [Culsans](https://github.com/x42005e1f/culsans) queues (best async Python queue out there!) to handle data processing pipelines. The main use case is when data needs to be processed efficiently and in a distributed manner but doesn't warrant the creation of more complex setups using Spark or Apache Airflow, or if you want a tool that's easier to run in native Python.

## Architecture

Bergtalzug implements a **dynamic multi-stage pipeline architecture** where you can define any number of processing stages, each with its own:

- **Execution model**: Async, ThreadPoolExecutor, ProcessPoolExecutor, or InterpreterPoolExecutor (Python 3.14+)
- **Worker count**: Configure concurrency per stage independently
- **Queue size**: Control backpressure with per-stage queue limits
- **Custom logic**: Each stage is a function you define

### Pipeline Flow

Data flows through stages as `WorkItem` objects. Each stage has its own pool of workers:

                    Stage1      Stage2      Stage3      Stage4
                    Stage1      Stage2      Stage3      Stage4
refill_queue()  ->  Stage1  ->  Stage2  ->  Stage3  ->  Stage4
                    Stage1      Stage2      Stage3      Stage4
                    Stage1      Stage2      Stage3      Stage4

Each `WorkItem` goes through the entire pipeline sequentially before being marked as "completed". Items are retrieved by implementing the `refill_queue()` method, which the pipeline calls automatically when the first stage's queue needs refilling. The size of each queue that feeds into each stage is defined in the Stage configuration you create.

### Benefits of Multi-Stage Design

The multi-stage architecture with separate worker pools provides several advantages:

1. **Buffering**: Data can be buffered between stages, allowing fast stages to continue working even when slower stages are backed up
2. **Resource Optimization**: I/O-heavy stages can use async workers, while CPU-heavy stages can use process pools
3. **Independent Scaling**: Each stage can be scaled independently based on its specific bottlenecks
4. **Better Throughput**: Workers in one stage can start on the next item immediately after finishing, without waiting for the entire pipeline

### Execution Types

Bergtalzug supports four execution models for maximum flexibility:

- **ASYNC**: Native asyncio for I/O-bound operations (API calls, database queries, file operations)
- **THREAD**: ThreadPoolExecutor for blocking I/O operations that can't use async
- **PROCESS**: ProcessPoolExecutor for CPU-intensive operations that need true parallelism
- **INTERPRETER**: InterpreterPoolExecutor for CPU-intensive operations with true multi-core parallelism (Python 3.14+ only)

You should only use `ThreadPoolExecutor` if for some reason `async` is not an option - e.g. the code you are executing releases the [GIL](https://en.wikipedia.org/wiki/Global_interpreter_lock) but doesn't have an `async` interface. If your code _doesn't_ release the GIL you can use either the `InterpreterPoolExecutor` or the `ProcessPoolExecutor`.

**New in Python 3.14**, `InterpreterPoolExecutor` provides true multi-core parallelism similar to ProcessPoolExecutor, but with each worker running in its own **isolated interpreter** instead of a separate process. Each interpreter has its own GIL, enabling genuine multi-core CPU utilization by spawning a new thread for each interpreter. Each interpreter is completely isolated - separate module imports, separate `sys.stdout`, etc. One of the biggest benefits is that `InterpreterPoolExecutor` supports zero-copy operations on various non-mutables types, e.g. `bytes`, `tuple` etc.

In general (assuming you use this framework for data processing) `InterpreterPoolExecutor` will always be a better pick than `ProcessPoolExecutor` since data can be passed without being pickled (avoid serialization overhead) and without holding more memory than required.

You can review more information on the different type of executors here:
 - https://docs.python.org/3/library/concurrent.futures.html
 - https://docs.python.org/3/library/concurrent.interpreters.html#module-concurrent.interpreters

## Usage Examples

One use case is downloading data (async operation), process it (CPU heavy operation) and store it (async operation). Now depending on what you are doing the pipeline could be fully `async` (e.g. the library/code you use to process the data supports an async interface) or a mix of `async` and `sync` (`InterpreterPoolExecutor` or `ProcessPoolExecutor` if interpreters can't be used).

### Example 1
Task: Download data, validate it with [DuckDB](https://duckdb.org/) and store it in S3.
This could be a full `async` pipeline with 3 stages since there is an `async` DuckDB library. So why not just have "1 big stage", what is even the point of using a pipeline here? Even if you use DuckDBs async interface it would still spawn real DuckDB instances in the background which utilize your CPU cores.

If you want to download 100 files at the same time and have each async task run in one streamlined process (download, process, upload) replicated 100 times, this would spawn 100 DuckDB instances which wouldn't be efficient at all and would only cause slow downs in processing. On the other hand you can spawn 100 download tasks, 20 processing (utilizing all of your cores) tasks and 100 upload tasks. If the files are big the download would take some time (Network I/O) but the processing in DuckDB could be quite fast hence you are doing all 3 tasks (download, process and upload) as efficient as possible.

### Example 2
Task: Download data, perform some calculations using [NumPy](https://numpy.org/) and save it to a file.

The first and last stages obviously should be `async` but the middle one is a bit more difficult. NumPy _does_ release the GIL for most operations but not for all. At the same time it does not provide an `async` interface so we now have to decide between threads, processes or interpreters. If the workload you have releases the GIL - use the `ThreadPoolExecutor` since it's easier to work with them (passing arguments is easier, no serialization etc.) and the underlying C library behind NumPy will utilize all cores anyway.

If your workload _doesn't_ release the GIL use the `InterpreterPoolExecutor` since this way you can run tasks that don't release the GIL concurrently without the serialization and copy overhead of the `ProcessPoolExecutor`. If for some reason you can't use interpreters - use processes.

> Note:
Of course you can add as many stages behind each other as you like but keep in mind that this adds overhead. It's advisable to complete the work in as little stages as possible.

# Quick Start

## 1. Define Your Pipeline Configuration

Create a configuration specifying your stages:

```python
from bergtalzug import ETLPipelineConfig, StageConfig, ExecutionType

config = ETLPipelineConfig(
    pipeline_name="my_pipeline",
    stages=[
        StageConfig(
            name="fetch",
            execution_type=ExecutionType.ASYNC,
            workers=10,
            queue_size=100, # size of the queue that feeds this stage - how many items can wait before being processed in this stage
        ),
        StageConfig(
            name="parse",
            execution_type=ExecutionType.THREAD,
            workers=5,
            queue_size=50,
        ),
        StageConfig(
            name="compute",
            execution_type=ExecutionType.PROCESS,
            workers=4,
            queue_size=20,
        ),
        StageConfig(
            name="store",
            execution_type=ExecutionType.ASYNC,
            workers=10,
            queue_size=100,
        ),
    ],
    queue_refresh_rate=1.0,  # Check first queue every second and call refill_queue()
    stats_interval_seconds=10.0,  # Report stats every 10 seconds
    enable_tracking=True,  # Track items and collect results
)
```

## 2. Create Your Pipeline Class

Inherit from `ETLPipeline` and implement `refill_queue()`:

```python
from bergtalzug import ETLPipeline, WorkItem

class MyPipeline(ETLPipeline):
    def __init__(self, config: ETLPipelineConfig):
        super().__init__(config)
        self.items_to_process = 100
        self.items_generated = 0

    async def refill_queue(self, count: int) -> list[WorkItem]:
        """
        Generate work items to feed into the pipeline.

        Args:
            count: Number of items requested by the pipeline

        Returns:
            List of WorkItems, or empty list to signal completion
        """
        items = []
        for _ in range(count):
            if self.items_generated >= self.items_to_process:
                break

            items.append(WorkItem(data={"id": self.items_generated, "url": f"https://api.example.com/item/{self.items_generated}"}))
            self.items_generated += 1

        return items  # Return empty list when no more work
```

## 3. Define Stage Handler Functions

Create a handler function for each stage. **Important**: The function signature depends on the execution type!

### For ASYNC and THREAD stages:

```python
# ASYNC handler - use async def
async def fetch_data(item: WorkItem) -> WorkItem:
    """Fetch data from API"""
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(item.data["url"]) as resp:
            item.data["raw_data"] = await resp.json()
    return item

# THREAD handler - use regular def
def parse_data(item: WorkItem) -> WorkItem:
    """Parse and validate data (blocking operation)"""
    import json
    parsed = json.loads(item.data["raw_data"])
    item.data["parsed"] = parsed
    return item
```

### ⚠️ For PROCESS and INTERPRETER stages:

**CRITICAL**: ProcessPoolExecutor and InterpreterPoolExecutor have **different function signatures**! This is due to a limitation where the `WorkItem` class can't be pickled and passed to these executors. Instead the job_id and data are passed separately, as a side-effect you do not have access to `WorkItem.metadata` - the metadata could potentially be passed as an optional argument if a flag is set in the config, please open an issue if this is something you think is needed.

If you get an `args not shareable` error it is very likely that you didn't import one of the dependencies you using in your function. E.g. if you use `time.sleep()` you need to call `import time` _inside_ the function not inside the module.

```python
# PROCESS handler example
def compute_data(job_id: str, data: Any) -> Any:
    """
    CPU-intensive computation using ProcessPoolExecutor.

    - Takes job_id and data as separate parameters (not WorkItem!)
    - Returns only the modified data (not WorkItem!)
    - Must be a module-level function (not a method or lambda since they can't be pickled)
    """
    logging.info(f"Processing {job_id}")

    # Do CPU-intensive work
    result = math.sqrt(data["value"])
    data["computed"] = result

    return data  # Return data only, not WorkItem

# INTERPRETER handler example (Python 3.14+)
def enrich_data(job_id: str, data: Any) -> Any:
    """
    CPU-intensive computation using InterpreterPoolExecutor.

    - Takes job_id and data as separate parameters (not WorkItem!)
    - Each interpreter is completely isolated
    - ALL imports must be inside the function
    """
    import math  # Must import inside - each interpreter is separate

    print(f"[INTERPRETER] Enriching {job_id}")

    # Do CPU-intensive work
    result = math.sqrt(data["value"]) * 2
    data["enriched"] = result

    return data  # Return data only, not WorkItem
```

### ASYNC handler for final stage:

```python
async def store_data(item: WorkItem) -> WorkItem:
    """Store results asynchronously"""
    # Save to database, upload to S3, etc.
    await save_to_database(item.data)
    return item
```

## 4. Register Handlers and Run

```python
import asyncio

async def main():
    # Create pipeline
    pipeline = MyPipeline(config)

    # Register handler for each stage
    pipeline.register_stage_handler("fetch", fetch_data)
    pipeline.register_stage_handler("parse", parse_data)
    pipeline.register_stage_handler("compute", compute_data)
    pipeline.register_stage_handler("store", store_data)

    # Optional: Add completion callback
    def on_complete(result):
        if result.success:
            print(f"✓ Item {result.job_id} completed in {result.total_duration:.2f}s")
        else:
            print(f"✗ Item {result.job_id} failed: {result.error}")

    pipeline.add_completion_callback(on_complete)

    # Run the pipeline (blocks until completion)
    results = await pipeline.run()

    # You can also start it in the background
    await pipeline.start()
    # Do some other actions...
    # And await until it's done
    results = await pipeline.run()

    # Analyze results
    successful = sum(1 for r in results if r.success)
    print(f"Processed {len(results)} items, {successful} successful")

if __name__ == "__main__":
    asyncio.run(main())
```

# Important Warnings and Best Practices

## Function Signatures

The function signature **differs based on execution type**:

| Execution Type | Function Signature | Returns |
|----------------|-------------------|---------|
| ASYNC | `async def handler(item: WorkItem) -> WorkItem` | `WorkItem` |
| THREAD | `def handler(item: WorkItem) -> WorkItem` | `WorkItem` |
| PROCESS | `def handler(job_id: str, data: Any) -> Any` | `Any` (data only) |
| INTERPRETER | `def handler(job_id: str, data: Any) -> Any` | `Any` (data only) |

The data returned by `PROCESS` and `INTERPRETER` executors will be packaged once again into the same `WorkItem` object.

**Why the difference?** ProcessPoolExecutor and InterpreterPoolExecutor require picklable data. The `WorkItem` object with all its metadata cannot be efficiently pickled, so we extract `job_id` and `data` before passing to these executors.

## Resource Considerations

- **Queue Sizes**: Larger queues buffer more work but use more memory
- **Worker Counts**:
  - ASYNC: Can be high (50-100+) for I/O-bound work
  - THREAD: Depends on your usecase, see examples
  - PROCESS: Should match CPU cores (use `os.cpu_count()`)
  - INTERPRETER: Should match CPU cores, similar to PROCESS
- **Memory**: Each worker & queue holds items in memory; monitor usage

## Error Handling

- Pipeline continues processing on errors - there is currently no DLQ (Dead-letter queue) or retry mechanism implemented.
- Failed items are tracked separately in results
- Use `enable_tracking=True` to get detailed error information
- Check `result.success` and `result.error` for each item

## Monitoring and Statistics

The pipeline logs messages by itself however if you don't want this you can disable it by setting `stats_interval_seconds` in `ETLPipelineConfig` to `0` and check the pipeline statistics yourself using the the `get_pipeline_stats()` function.

```python
# Non-blocking start
await pipeline.start()

# Monitor while running
while pipeline.is_running():
    stats = await pipeline.get_pipeline_stats()
    print(f"Active: {stats['active_items']}, Completed: {stats['completed_items']}")
    print(f"Success rate: {stats['success_rate']:.2%}")
    await asyncio.sleep(1)

# Wait for completion
results = await pipeline.run()
```

After the pipeline finished you check the results for durations and other data for each stage.

```python
for result in results:
    print(f"Job {result.job_id}:")
    print(f"  Total: {result.total_duration:.2f}s")
    for stage_name in ["fetch", "parse", "compute", "store"]:
        duration = result.metadata.get_stage_duration(stage_name)
        if duration:
            print(f"  {stage_name}: {duration:.2f}s")
```

Additionally each `WorkItem` has a metadata in the form of a `WorkItemMetadata` object. You can access it during runtime to check each workitems status, it's a good idea to keep a reference to workitems after you submit them in your `refill_queue()` function in case you want to monitor them.

## Complete Example

See `example.py` for a complete working example demonstrating all execution types and features.

## Docker Support

An example Dockerfile is provided showing how to use Bergtalzug's base Docker image.
