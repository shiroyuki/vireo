import logging

_logger      = None
_log_handler = None


def log(level, *args, **kwargs):
    global _logger

    if not _logger:
        prepare_logger(logging.WARNING)

    getattr(_logger, level)(*args, **kwargs)


def prepare_logger(level = logging.WARNING):
    global _logger

    formatter = logging.Formatter('{name} @ {asctime} [{levelname}] {message}', style='{')

    _logger = logging.getLogger('vireo')
    _logger.setLevel(level)

    log_handler = logging.StreamHandler()
    log_handler.setLevel(level)
    log_handler.setFormatter(formatter)

    _logger.addHandler(log_handler)

    return _logger

def fill_in_the_blank(main_dict, default_dict):
    for default_key, default_value in default_dict.items():
        if default_key in main_dict:
            continue

        main_dict[default_key] = default_value

    return main_dict
