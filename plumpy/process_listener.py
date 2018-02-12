from abc import ABCMeta

from future.utils import with_metaclass
from builtins import str

__all__ = ['ProcessListener']


class ProcessListener(with_metaclass(ABCMeta, object)):
    def on_process_created(self, process):
        """
        Called when the process has been started

        :param process: The process
        :type process: :class:`plumpy.Process`
        """
        pass

    def on_process_running(self, process):
        """
        Called when the process is about to enter the RUNNING state

        :param process: The process
        :type process: :class:`plumpy.Process`
        """
        pass

    def on_process_waiting(self, process, data):
        """
        Called when the process is about to enter the WAITING state

        :param process: The process
        :type process: :class:`plumpy.Process`
        """
        pass

    def on_process_paused(self, process):
        """
        Called when the process is about to re-enter the RUNNING state

        :param process: The process
        :type process: :class:`plumpy.Process`
        """
        pass

    def on_process_played(self, process):
        """
        Called when the process is about to re-enter the RUNNING state

        :param process: The process
        :type process: :class:`plumpy.Process`
        """
        pass

    def on_output_emitted(self, process, output_port, value, dynamic):
        """
        Called when the process has emitted an output value

        :param process: The process
        :type process: :class:`plumpy.Process`
        :param output_port: The output port that the value was outputted on
        :type output_port: str
        :param value: The value that was outputted
        :param dynamic: True if the port is dynamic, False otherwise
        :type dynamic: bool
        """
        pass

    def on_process_finished(self, process, outputs):
        """
        Called when the process has been aborted

        :param process: The process
        :type process: :class:`plumpy.Process`
        :param outputs: The process outputs
        """
        pass

    def on_process_failed(self, process, exc_info):
        """
        Called when the process has finished running successfully

        :param process: The process
        :type process: :class:`plumpy.Process`
        """
        pass

    def on_process_cancelled(self, process, msg):
        """
        Called when the process is about to enter the STOPPED state

        :param process: The process
        :type process: :class:`plumpy.Process`
        """
        pass
