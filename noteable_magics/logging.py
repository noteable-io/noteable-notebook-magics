import logging

import structlog
from structlog.contextvars import merge_contextvars


def rename_event_key(logger, method_name, event_dict):
    """Renames the `event` key to `message`

    This helper function renames the `event` key in structured logging
    entries to `message` key which conforms to Datadog's default
    attribute for log message text.
    """
    event_dict["message"] = event_dict.pop("event")
    return event_dict


def configure_logging(dev_logging: bool, ext_log_level, app_log_level) -> None:
    """A helper function to configure structured logging and root logger"""
    shared_processors = [
        merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if dev_logging:
        renderer = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()
        shared_processors.append(rename_event_key)

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer, foreign_pre_chain=shared_processors
    )

    structlog_processors = [
        structlog.stdlib.filter_by_level,
        *shared_processors,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    structlog.configure(
        processors=structlog_processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    try:
        handler = logging.FileHandler("/var/log/noteable_magics.log")
    except PermissionError:
        # Locally the user may not have permission to the /var/log directory
        handler = logging.FileHandler("/tmp/noteable_magics.log")

    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(ext_log_level)
    root_logger.handlers = []
    root_logger.addHandler(handler)

    # We generally want more verbose logs for logs we generate
    # and less verbose for external packages
    logging.getLogger("noteable_magics").setLevel(app_log_level)
