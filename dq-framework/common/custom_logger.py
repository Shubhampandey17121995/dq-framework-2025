import logging
def getlogger():
    logger = logging.getLogger()

    if logger.hasHandlers():
        logger.handlers.clear()

    log_format = "[%(asctime)s] [%(levelname)s] %(message)s"

    logging.basicConfig(format = log_format,datefmt = "%Y-%m-%d %H:%M:%S",level = logging.INFO)
    return logger