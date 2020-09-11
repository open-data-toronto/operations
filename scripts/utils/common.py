import logging
import sys


def setup_custom_logger(name, log_file_path):
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.FileHandler(log_file_path, mode="a")
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)

    return logger
