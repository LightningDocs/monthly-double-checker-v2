import logging
from pathlib import Path
from datetime import datetime


def initialize_logger() -> logging.Logger:
    """Sets up a logger with handlers to the console and the /logs/ folder.
    Also ensures that a folder called ./logs/ is created if not already existing.

    Returns:
        logging.Logger: An customized instance of the Logger class.
    """
    Path("./logs").mkdir(exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    logger = logging.getLogger(__name__)
    logger.setLevel("DEBUG")

    console_handler = logging.StreamHandler()
    console_handler.setLevel("DEBUG")
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(f"logs/{today}.txt", mode="w", encoding="utf-8")
    file_handler.setLevel("INFO")
    file_formatter = logging.Formatter(
        style="{",
        fmt="[{asctime}] {levelname} - {message}",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger
