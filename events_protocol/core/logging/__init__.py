import json
import logging
import queue
import re
import sys
import typing
from logging.handlers import QueueHandler, QueueListener

from events_protocol.core.context import EventContextHolder


def _get_klass_name(klass: typing.Any) -> str:
    # Simple and effective
    return re.sub(r"|".join(map(re.escape, ["<class", "'", ">", " "])), "", str(klass))


_logger = None


def _get_logger():
    global _logger
    if _logger:
        return _logger
    logging.basicConfig(level=logging.INFO)
    _queue = queue.Queue(-1)
    _queue_handler = QueueHandler(_queue)
    _handler = logging.StreamHandler(sys.stdout)
    _queue_listener = QueueListener(_queue, _handler)

    _logger = logging.getLogger("gb.events_protocol")
    if _logger.hasHandlers():
        _logger.handlers.clear()
    _logger.addHandler(_queue_handler)
    _logger.propagate = False
    _queue_listener.start()
    return _logger


class JsonLogger(logging.LoggerAdapter):
    version: str = ""

    @classmethod
    def set_version(cls, version: str):
        cls.version = version

    def __init__(self, klass=None):
        self.logger = _get_logger()
        self.klass = _get_klass_name(klass)

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            event_context = EventContextHolder.get()
            _msg = dict(
                severity=logging.getLevelName(level),
                LogMessage=msg,
                EventId=event_context.id,
                FlowID=event_context.flow_id,
                EventVersion=event_context.event_version,
                UserId=event_context.user_id,
                Operation=event_context.event_name,
                Logger=self.klass,
                LoggerName=self.logger.name,
                ApplicationVersion=self.version,
            )

            extra = kwargs.pop("extra", None)
            if extra:
                _msg["Extra"] = extra
            if level == logging.ERROR and kwargs.get("exc_info"):
                args = tuple()
                fmt = logging.Formatter()
                _exc = sys.exc_info()
                _msg["StackTrace"] = fmt.formatException(_exc).split("\n")

                kwargs["exc_info"] = False
            msg = json.dumps(_msg)
            self.logger.log(level, msg, *args, **kwargs)
