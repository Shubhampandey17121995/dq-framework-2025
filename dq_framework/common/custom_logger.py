import logging
def getlogger():
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()
    logging.basicConfig(format = "%(asctime)s | %(levelname)s | %(message)s\n",datefmt = "%Y-%m-%d %H:%M:%S",level = logging.INFO)
    return logger