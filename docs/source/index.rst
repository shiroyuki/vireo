.. Vireo documentation master file, created by
   sphinx-quickstart on Tue Mar  7 10:51:45 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Vireo
#####

A library and framework for event-driven application development

.. warning::

    This library is currently under active development. Until version 1.0, the API signatures are
    subject to change. While the code should be stable (not easily throwing exceptions) even before
    reaching version 1.0, version pinning is highly recommended.


API Reference
=============

Core and Observer
-----------------

.. automodule:: vireo.core
    :members:

.. automodule:: vireo.observer
    :members:

RabbitMQ Driver
---------------

.. autoclass:: vireo.drivers.rabbitmq.driver.Driver
    :members:

.. autoclass:: vireo.drivers.rabbitmq.consumer.Consumer
    :members:

.. autoclass:: vireo.drivers.rabbitmq.exception.NoConnectionError
    :members:

.. autoclass:: vireo.drivers.rabbitmq.exception.SubscriptionNotAllowedError
    :members:


.. Contents:
..
.. .. toctree::
..    :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
