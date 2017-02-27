# Vireo

A library and framework for event-driven application development.

> **Caution:** This library is currently under active development. Until version 1.0, the API signatures are subject to change. While the code should be stable (not easily throwing exceptions) even before reaching version 1.0, version pinning is highly recommended.

## Prerequisites

| Software or library | Minimum supported version | Extensively tested version |
| ------------------- | ------------------------- | -------------------------- |
| Python (cpython)    | 2.7 or 3.5                | 3.5 for MacOS and Fedora   |
| pika                | 0.10                      | 0.10 for MacOS and Fedora  |
| RabbitMQ            | 3.6                       | 3.6 supplied via Docker    |

## Future supports (if anyone happens to be interested)

* Amazon Kinesis Stream
* Apache Kafka

## Introduction

**Vireo** is a simple library and framework for event-driven applications to stop the classic DRY problem and enable developers to focus on developing applications rather than setting up the base code. There are two modes: a write-only Vireo, called **Core** (`vireo.Core`), and a read-write Vireo, called **Observer** (`vireo.Observer` which is a subclass of `vireo.Core`).

The design is inspired by Node.js event API.

### Features

* Simplify event-driven application development.
* Provide the delayed message handling.

> The delayed message handling is heavily relying on **dead letter exchange (DLX)** when Vireo is used with RabbitMQ.

### Roadmap (Soon to be implemented):

* the factory class to simplify the setup for either an emitter or an observer.

## Getting started

### Without a factory class

You can simply write.

```python
from vireo          import Observer # or Core
from vireo.driver   import AsyncRabbitMQDriver
from vireo.observer import Observer, SYNC_START

driver = AsyncRabbitMQDriver('amqp://guest:guest@localhost:5672/%%2F')
app    = Observer(driver) # or Core(driver)
```

### With a factory class (proposal)

*(Coming soon)*

You can simply write.

```python
from vireo import ObserverFactory # or CoreFactory

app = CoreFactory('amqp://guest:guest@localhost:5672/%%2F')
```

## APIs

### `vireo.Core.emit(event_name, data = None, options = None)`

Emit a message to the given event.

```python
app.emit('security.alert.intrusion', {'ip': '127.0.0.1'})
```

### `vireo.Observer.open(event_name, options = None, delegation_ttl = None)`

Prepare to observe an event.

If `delegation_ttl` is `None`, the delegation will not be enabled. Otherwise, it will be enabled.

To enable delegation, ``delegation_ttl`` must not be set to ``None`` or ``0`` or ``False``.

The delegation of the event happens when there exists no listener to that event. Then,
the message will be transfered to the delegated event, which is similar to the given
event name but suffixed with ``.delegated``. For instance, given an event name
"outbound_mail_delivery", the delegated event name will be "outbound_mail_delivery.delegated".

To handle the delegated event, simply listen to the delegated event. For example,
continuing from the previous example, you can write ``on('outbound_mail_delivery.delegated', lambda x: foo(x))``.

```python
app.open('foo')
```

### `vireo.Observer.close(event_name, options = None)`

Clean up after observing an event.

### `vireo.Observer.on(event_name, callback)`

Listen to an event with a callback function.

The callback is a callable object, e.g., function, class method, lambda object, which
takes only one parameter which is a JSON-decoded object.

```python
def on_foo(self, message):
    print('on_foo:', message)

app.on('foo', on_foo)
app.on('foo.lambda', lambda x: print('foo_lambda:', x))
```
