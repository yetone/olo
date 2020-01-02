import logging

logger = logging.getLogger(__name__)
if not logger.handlers:
    rht = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(filename)s %(funcName)s %(lineno)s \
        %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    rht.setFormatter(fmt)
    logger.addHandler(rht)
logger.setLevel(logging.INFO)
