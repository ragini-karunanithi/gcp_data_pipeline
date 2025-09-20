import logging

def notify_success(msg: str):
    logging.info(f"SUCCESS: {msg}")

def notify_failure(msg: str):
    logging.error(f"FAILURE: {msg}")
