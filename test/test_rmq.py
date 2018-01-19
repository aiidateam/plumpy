try:
    import pika
    import pika.exceptions
    from plum.rmq.launch import RmqTaskPublisher, RmqTaskSubscriber

    _HAS_PIKA = True
except ImportError:
    _HAS_PIKA = False
