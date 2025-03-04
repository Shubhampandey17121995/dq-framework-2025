import logging
def getlogger(batch_id):
    logger = logging.getLogger()

    if logger.hasHandlers():
        logger.handlers.clear()

    log_format = "[%(asctime)s] [%(levelname)s] [Batch ID: %(batch_id)s] %(message)s"

    logging.basicConfig(format = log_format,datefmt = "%Y-%m-%d %H:%M:%S",level = logging.INFO)
    return logger