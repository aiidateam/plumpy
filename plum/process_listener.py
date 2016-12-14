from abc import ABCMeta


class ProcessListener(object):
    __metaclass__ = ABCMeta

    def on_process_start(self, process):
        pass

    def on_process_run(self, process):
        pass

    def on_process_wait(self, process):
        pass

    def on_process_resume(self, process):
        pass

    def on_output_emitted(self, process, output_port, value, dynamic):
        pass

    def on_process_finish(self, process):
        pass

    def on_process_stop(self, process):
        pass

    def on_process_fail(self, process):
        pass
