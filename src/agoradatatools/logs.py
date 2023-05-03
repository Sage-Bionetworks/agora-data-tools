import time

from . import logger


def format_seconds(seconds):
    """Converts a floating-point seconds value to format hh:mm:ss format."""
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def log_time(config: str):
    """Wrapper that calculates how long it takes for a function to run and then logs that time.

    Args:
        config (str): Name of function to be wrapped.
        Set up configurations for process_dataset and process_all_files functions

    Raises:
        ValueError: If the configuration is not supported, error is raised.

    Returns:
        log: log containing timestamp for function run.
    """
    if config not in ["process_dataset", "process_all_files"]:
        raise ValueError(f"configuration {config} not supported for log_time wrapper.")

    def log(func):
        def wrapped(*args, **kwargs):
            # start timing
            start_time = time.monotonic()
            func(*args, **kwargs)
            # Record the end time
            end_time = time.monotonic()
            # Calculate the elapsed time
            elapsed_time = round(end_time - start_time, 2)
            elapse_time_formatted = format_seconds(elapsed_time)
            # logger = create_logger()
            if config == "process_dataset":
                dataset = next(iter(kwargs["dataset_obj"]))
                string_list = [elapse_time_formatted, dataset]
                message = "Elapsed time: %s for %s dataset"

            if config == "process_all_files":
                string_list = [elapse_time_formatted]
                message = "Elapsed time: %s for all data processing"

            logger.info(message, *string_list)

        return wrapped

    return log
