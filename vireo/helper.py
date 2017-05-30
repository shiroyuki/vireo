import logging

_logger      = None
_log_handler = None


def log(level, *args, **kwargs):
    global _logger

    if not _logger:
        prepare_logger(logging.ERROR)

    getattr(_logger, level)(*args, **kwargs)


def prepare_logger(level = logging.ERROR):
    global _logger
    global _log_handler

    if not _logger:
        _logger = logging.Logger('vireo')

    if not _log_handler:
        try:
            # Python 3
            formatter = logging.Formatter('{name} @ {asctime} [{levelname}] {message}', style='{')
        except TypeError:
            # Python 2
            formatter = logging.Formatter('$(name)a @ $(asctime)s [%(levelname)s] %(message)s')

        _log_handler = logging.StreamHandler()
        _log_handler.setFormatter(formatter)

        _logger.addHandler(_log_handler)

    _logger.setLevel(level)
    _log_handler.setLevel(level)

    return _logger

def fill_in_the_blank(main_dict, default_dict):
    for default_key, default_value in default_dict.items():
        if default_key in main_dict:
            continue

        main_dict[default_key] = default_value

    return main_dict
