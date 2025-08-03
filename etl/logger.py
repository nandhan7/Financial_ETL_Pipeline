import logging
import os


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console Handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    # File Handler
    # 'a' to append, 'w' to overwrite
    fh = logging.FileHandler("logs/etl_pipeline.log", mode='a')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    # Add handlers only if not already added
    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger
