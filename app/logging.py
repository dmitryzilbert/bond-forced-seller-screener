import logging
from logging.config import dictConfig
from .settings import get_settings


def setup_logging() -> None:
    settings = get_settings()
    dictConfig(
        {
            "version": 1,
            "formatters": {
                "default": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                }
            },
            "root": {
                "handlers": ["console"],
                "level": settings.log_level,
            },
        }
    )
    logging.getLogger(__name__).debug("Logging configured")
