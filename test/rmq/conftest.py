import pytest
import shortuuid

from kiwipy import rmq
from plumpy import process_comms


@pytest.yield_fixture
def communicator() -> rmq.RmqThreadCommunicator:
    message_exchange = '{}.{}'.format(__file__, shortuuid.uuid())
    task_exchange = '{}.{}'.format(__file__, shortuuid.uuid())
    queue_name = '{}.{}.tasks'.format(__file__, shortuuid.uuid())

    communicator = rmq.connect(
        connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
                                        message_exchange=message_exchange,
                                        task_exchange=task_exchange,
                                        task_queue=queue_name,
                                        testing_mode=True)
    yield communicator
    communicator.stop()


@pytest.yield_fixture
def controller(communicator):
    yield process_comms.RemoteProcessController(communicator)


@pytest.yield_fixture
def controller_thread(communicator):
    yield process_comms.RemoteProcessThreadController(communicator)
