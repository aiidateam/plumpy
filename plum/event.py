# -*- coding: utf-8 -*-

from abc import ABCMeta


class ProcessListener(object):
    __metaclass__ = ABCMeta

    def process_starting(self, process, inputs):
        pass

    def process_finished(self, process, outputs):
        pass


class WorkflowListener(object):
    __metaclass__ = ABCMeta

    def process_adding(self, workflow, process, local_name):
        pass

    def process_added(self, workflow, process, local_name):
        pass

    def process_removed(self, workflow, local_name):
        pass

    def link_created(self, workflow, source, sink):
        pass

    def link_removed(self, workflow, source, sink):
        pass

    def workflow_starting(self, workflow):
        pass

    def workflow_finished(self, workflow, outputs):
        pass

    def subprocess_starting(self, workflow, process, inputs):
        pass

    def subprocess_finished(self, workflow, process, outputs):
        pass

    def value_outputted(self, workflow, value, source, sink):
        pass
