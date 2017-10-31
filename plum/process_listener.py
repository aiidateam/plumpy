from abc import ABCMeta

from future.utils import with_metaclass

class ProcessListener(with_metaclass(ABCMeta, object)):

    def on_process_start(self, process):
        """
        Called when the process has been started

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        pass

    def on_process_run(self, process):
        """
        Called when the process is about to enter the RUNNING state

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        pass

    def on_process_wait(self, process):
        """
        Called when the process is about to enter the WAITING state

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        pass

    def on_process_resume(self, process):
        """
        Called when the process is about to re-enter the RUNNING state

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        pass

    def on_process_abort(self, process):
        """
        Called when the process has been aborted

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        pass

    def on_output_emitted(self, process, output_port, value, dynamic):
        """
        Called when the process has emitted an output value

        :param process: The process
        :type process: :class:`plum.process.Process`
        :param output_port: The output port that the value was outputted on
        :type output_port: basestring
        :param value: The value that was outputted
        :param dynamic: True if the port is dynamic, False otherwise
        :type dynamic: bool
        """
        pass

    def on_process_finish(self, process):
        """
        Called when the process has finished running successfully

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        pass

    def on_process_stop(self, process):
        """
        Called when the process is about to enter the STOPPED state

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        pass

    def on_process_fail(self, process):
        """
        Called when the process is about to enter the FAILED state

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        pass

    def on_process_terminate(self, process):
        """
        Called when the process has terminated, either because of finishing, being
        aborted or failing.

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        pass
