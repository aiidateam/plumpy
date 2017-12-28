try:
    import pika
    import pika.exceptions
    from plum.rmq.launch import ProcessLaunchPublisher, ProcessLaunchSubscriber
    from plum.rmq.status import ProcessStatusSubscriber

    _HAS_PIKA = True
except ImportError:
    _HAS_PIKA = False
