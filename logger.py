# Created by vinod on 17 Aug, 2020

import datetime
import logging
import logging.config
import os
import sys

import structlog
from structlog import wrap_logger
from structlog.processors import JSONRenderer
from structlog.processors import StackInfoRenderer
from structlog.processors import TimeStamper
from structlog.processors import UnicodeDecoder
from structlog.processors import format_exc_info
from structlog.stdlib import add_log_level
from structlog.stdlib import add_logger_name
from structlog.stdlib import filter_by_level

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(stream=sys.stdout, format="%(message)s", level=LOG_LEVEL)

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "format": "%(message)s %(lineno)d %(pathname)s",
                "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
            }
        },
    }
)


def convert_pb_message_to_dict(obj):
    new_obj = obj
    if isinstance(obj, dict):
        new_obj = {}
        for key in obj:
            new_obj[key] = convert_pb_message_to_dict(obj[key])
    elif isinstance(obj, (list, tuple)):
        new_obj = [convert_pb_message_to_dict(item) for item in obj]
    # elif isinstance(obj, message.Message):
    #     new_obj = MessageToDict(obj)
    elif hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
        new_obj = obj.to_dict()
    elif hasattr(obj, "toDict") and callable(getattr(obj, "toDict")):
        new_obj = obj.toDict()
    return new_obj


def rename_for_stackdriver(logger, log_method, event_dict):
    event_dict["message"] = event_dict.get("event", "")
    event_dict["severity"] = event_dict.get("level", LOG_LEVEL)
    event_dict.pop("event")
    event_dict.pop("level")
    return event_dict


def add_timestamp(logger, log_method, event_dict):
    event_dict["timestamp"] = datetime.datetime.utcnow()
    return event_dict


def get_logger(name):
    if not structlog.is_configured():
        structlog.configure_once(
            processors=[
                filter_by_level,
                add_log_level,
                add_logger_name,
                TimeStamper(fmt="iso"),
                rename_for_stackdriver,
                StackInfoRenderer(),
                format_exc_info,
                UnicodeDecoder(),
                JSONRenderer(sort_keys=True),
            ]
        )
    return wrap_logger(logging.getLogger(name))
