# Bergtalzug

Bergtalzug is a framework that fetches, processes and stores data (essentially an ETL framework) concurrently and as efficiently as possible in Python. To do that it utilizes multiple worker pools and [Culsans](https://github.com/x42005e1f/culsans) queues (best async Python queue out there!). The main usecase is when data needs to be processed efficiently and in a distributed manner but doesn't warrant the creation of more complex setups using Spark or Apache Airflow or if you want to use a tool for processing that is easier to run in native Python.

## Architecture

In general the setup is quite simple. The `ETLPipeline` class is an abstract class with a few functions that need to be overwritten. There are currently 3 stages, each is essentially a function you define:

 - `fetch` - will be asynchronously executed to fetch data
 - `process` - will be asynchronously executed to process said data
 - `process_sync` - use this function if your code cannot be run asynchronously (a Thread will be used)*
 - `store` - will be asynchronously executed to store the data

*: You can't define both `process` and `process_sync` as of now

As of now the stages are essentially very similar and you could write fetching logic in the `process` function but this would be an antipattern. The "Why" is explain below.

The pipeline is designed as follows: There are pools of workers where data is passed through. Data is passed in the form of `WorkItem` objects.

    `fetch`    `process`    `store`
    `fetch`    `process`    `store`
    `fetch` -> `process` -> `store`
    `fetch`    `process`    `store`
    `fetch`    `process`    `store`

Each job (a `WorkItem` object) goes through the whole pipeline before it's marked as "completed". To get jobs another function you need to overwite (`refill_queue`) is called.

An example setup would be a Queue service (or a Json file) that is called/read by your `refill_queue` function which returns a `WorkItem` object. This object is then passed by the pipeline to one of the `fetch` functions in the pool. This could be for example a URL that needs to be fetched. After your function finishes and returns the modified `WorkItem` object (for example the downloaded data) it is passed to the `process` function which does work on the downloaded data. Once that function is done and retuns the yet again modified `WorkItem` object. The `store` function is invoked with that new data - here you would store that data and this job is marked is done.

Now even though `fetch` is a singular function you can write as much code or call other function from within, this applies to all other functions. Workload that is IO heavy (fetching and storing) should be in the `fetch` and `store` functions. CPU heavy load should be in the `process` function.

The benefit of doing it this way instead of having just 1 single big pool of concurrently running workers is that data can be buffered between pools. E.g. if the processing work is heavy and takes a lot of time once a `process` function finishes executing it can start working on the next job instantly. If there would be no pools and just workers that do all the steps in one go you would could only do one thing at a time.

In the future there will be a pipeline builder that allow you to create as many stages with whatever properties you like dynamically - for example 2 async stages that then flow into a process stage where each worker has a single CPU core etc. etc. This would be possible using the new [`concurrent.interpreters`](https://docs.python.org/3.14/library/concurrent.interpreters.html#module-concurrent.interpreters)

## Example usecase

Let's say you need to download data, analyze + validate it with [DuckDB](https://duckdb.org/) and store it on a network drive. You could download in the `fetch` function, have the validation + analysis which is CPU heavy run in the `process` or `process_sync` function (You would use `process_sync` if your code can't release the [GIL](https://en.wikipedia.org/wiki/Global_interpreter_lock)). And finally upload the data in the `store` function.

# Code example

To get started you can take a look at `example.py` which shows how an example pipeline would look like. Each pipeline accepts a `ETLPipelineConfig`:

```python
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
```

This is where you define how big each pool and queue is as well as how often to refill the queues.
Now you can inherting from the `ETLPipeline` class and overwrite the abstract functions:

```python
class CustomETLPipeline(ETLPipeline):
    async def refill_queue(self, count: int) -> list[WorkItem]:
        # here you return a list of WorkItems. `count` is the
        # amount of items that the pipeline requested.
        # If there is nothing to do you can signal the pipeline to end with an empty list

    async def fetch(self, item: WorkItem) -> WorkItem:
        # Here comes your fetch logic
        # After you're done add the new data in the WorkItem
        return item

    async def process(self, item: WorkItem) -> WorkItem:
        # Here comes your process logic
        # After you're done add the new data in the WorkItem
        return item

    async def store(self, item: WorkItem) -> None:
        # Here comes your store logic

```

Additinally there is an ExampleDockerfile which shows how to use Bergtalzugs base Docker image.

# TODO: Add test for unknown job IDs (happens if refill queue incorrectly returns duplicate work items)
