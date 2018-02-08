from ...exception import ObservationError
from ...exception import NoConnectionError as BaseNoConnectionError


class NoConnectionError(BaseNoConnectionError):
    """ No connection error """


class SubscriptionNotAllowedError(ObservationError):
    """ Subscription not allowed """
