from __future__ import annotations

import logging
import random
import time
from pathlib import Path

from logstash_async.handler import AsynchronousLogstashHandler


WORDS = [
    "spark",
    "elastic",
    "kibana",
    "logstash",
    "python",
    "cluster",
    "dashboard",
    "search",
    "index",
    "query",
]

SENTENCE_TEMPLATES = [
    "streaming data improves {word} analysis",
    "the {word} pipeline stays healthy",
    "students use {word} for this homework",
    "the {word} dashboard highlights trends",
    "{word} search helps debug events",
]


def build_logger(database_path: Path) -> logging.Logger:
    logger = logging.getLogger("python-logstash-homework")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(
        AsynchronousLogstashHandler(
            host="127.0.0.1",
            port=5959,
            database_path=str(database_path),
        )
    )
    return logger


def emit_messages(logger: logging.Logger, total_messages: int = 120) -> None:
    random.seed(42)
    for idx in range(total_messages):
        word = random.choices(
            WORDS,
            weights=[14, 11, 7, 10, 8, 5, 4, 6, 9, 3],
            k=1,
        )[0]
        sentence = random.choice(SENTENCE_TEMPLATES).format(word=word)
        level = random.choice([logging.INFO, logging.WARNING, logging.ERROR])

        # Roughly one quarter of events should be filtered out by Logstash.
        prefix = "python-filebeat" if idx % 4 == 0 else "python-logstash"
        logger.log(
            level,
            f"{prefix}: {sentence}",
            extra={"top_word": word, "sentence": sentence},
        )
        time.sleep(0.02)


def close_logger(logger: logging.Logger) -> None:
    for handler in logger.handlers:
        handler.flush()
        handler.close()


def main() -> None:
    logger = build_logger(Path("logstash_events.db"))
    try:
        emit_messages(logger)
    finally:
        close_logger(logger)


if __name__ == "__main__":
    main()
