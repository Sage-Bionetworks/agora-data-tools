import logging
import time


def format_seconds(seconds):
    """Converts a floating-point seconds value to format hh:mm:ss format."""
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def time_function(func, *args, **kwargs):
    """Returns the elapsed time for a function to run."""
    start_time = time.monotonic()
    result = func(*args, **kwargs)
    end_time = time.monotonic()
    elapsed_time = end_time - start_time
    elapsed_time_formatted = format_seconds(elapsed_time)
    return elapsed_time_formatted, result


def log_time(func_name: str, logger: logging.Logger):
    """Decorator function that calculates how long it takes for a function to run and then logs that time.

    Args:
        func_name (str): Name of function to be wrapped.
        Set up configurations for process_dataset and process_all_files functions
        logger (logging.Logger): logger initialized in the module this decorator is used in

    Raises:
        ValueError: If the configuration is not supported, error is raised.

    Returns:
        log: log containing timestamp for function run.
    """
    if func_name not in ["process_dataset", "process_all_files"]:
        raise ValueError(
            f"configuration {func_name} not supported for log_time decorator."
        )

    def log(func):
        def wrapped(*args, **kwargs):
            if func_name == "process_dataset":
                dataset = next(iter(kwargs["dataset_obj"]))
                logger.info("Now processing %s dataset", dataset)
                elapsed_time_formatted, result = time_function(func, *args, **kwargs)
                logger.info("Processing complete for %s dataset", dataset)
                string_list = [elapsed_time_formatted, dataset]
                message = "Elapsed time: %s for %s dataset"

            if func_name == "process_all_files":
                logger.info("Agora Data Tools processing has started")
                elapsed_time_formatted, result = time_function(func, *args, **kwargs)
                logger.info("Agora Data Tools processing has completed")
                string_list = [elapsed_time_formatted]
                message = "Elapsed time: %s for all data processing"

            logger.info(message, *string_list)
            return result

        return wrapped

    return log
