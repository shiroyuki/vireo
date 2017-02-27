import logging

_logger      = None
_log_handler = None


def log(level, *args, **kwargs):
    global _logger

    if not _logger:
        prepare__logger(logging.INFO)

    getattr(_logger, level)(*args, **kwargs)

def prepare_logger(level):
    global _logger

    _logger = logging.getLogger('vireo')
    _logger.setLevel(level)

    _log_handler = logging.StreamHandler()
    _log_handler.setLevel(level)

    _logger.addHandler(_log_handler)


def fill_in_the_blank(main_dict, default_dict):
    for default_key, default_value in default_dict.items():
        if default_key in main_dict:
            continue

        main_dict[default_key] = default_value

    return main_dict
