from __future__ import annotations

import logging
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Market data worker placeholder started.")
    logger.info("MARKET_EVENT_STREAM_NAME=%s", os.getenv("MARKET_EVENT_STREAM_NAME"))
    logger.info("CANDIDATES_TABLE_NAME=%s", os.getenv("CANDIDATES_TABLE_NAME"))
    logger.info("Implement the WebSocket collector here, then put normalized events to Kinesis.")
    while True:
        time.sleep(300)


if __name__ == "__main__":
    main()
