"""JSON logs with a PII scrubber. Use log_event(logger, "event_name", key=value, ...)."""
import json
import logging
import sys
from datetime import datetime, timezone

PII_KEYS = {"employee_name", "name_of_employee", "phone", "mobile", "email", "address",
            "pickup_address", "drop_address", "lat", "lng", "location"}


def scrub(fields: dict) -> dict:
    return {k: ("***" if k.lower() in PII_KEYS else v) for k, v in fields.items()}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        out.update(scrub(getattr(record, "fields", {}) or {}))
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields) -> None:
    logger.log(level, event, extra={"fields": fields})
