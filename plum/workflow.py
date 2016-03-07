# -*- coding: utf-8 -*-

from abc import ABCMeta
import plum.event as event
from plum.port import Port, BindingPort
from plum.process import Process
import plum.util as util


class WorkflowOutputPort(BindingPort):
    def __init__(self, process, name, type=None):
        super(WorkflowOutputPort, self).__init__(process, name, type)


class ProcessLink(object):
    def __init__(self, source_output, sink_input):
        assert(isinstance(source_output, Port))
        assert(isinstance(sink_input, BindingPort))

        self._source = source_output
        self._sink = sink_input

    @property
    def source(self):
        return self._source

    @property
    def sink(self):
        return self._sink


class Workflow(Process):
    __metaclass__ = ABCMeta

    def __init__(self, name):
        super(Workflow, self).__init__(name)
        self._workflow_events = util.EventHelper(event.WorkflowListener)
        self._processes = {}
        self._links = {}

    def add_workflow_listener(self, listener):
        self._workflow_events.add_listener(listener)

    def remove_listener(self, listener):
        self._workflow_events.remove_listener(listener)

    def add_process(self, process, local_name=None):
        name = local_name if local_name else process.name
        self._processes[name] = process
        for l in self._workflow_events.listeners:
            l.process_added(self, process, local_name)

    def remove_process(self, local_name):
        self._processes.pop(local_name)
        for l in self._workflow_events.listeners:
            l.process_removed(self, local_name)

    def get_processes(self):
        return self._processes

    def add_output(self, name, **kwargs):
        """
        Add an output port to the workflow.

        :param name: The name of the output port.
        :param kwargs: Keyword arguments supported by output ports
        """
        if name in self._outputs:
            raise ValueError("Output {} already exists.".format(name))

        self._outputs[name] = WorkflowOutputPort(self, name, *kwargs)

    def link(self, source, sink):
        if source in self.get_links():
            raise ValueError("Link from {} already exists, remove first")

        source_name, source_output = source.split(':')
        sink_name, sink_input = sink.split(':')

        if not source_name:
            source_port = self.get_input(source_output)
        else:
            try:
                source_port =\
                    self.get_processes()[source_name].get_output(source_output)
            except KeyError:
                raise ValueError("Invalid link {} -> {}".format(source, sink))

        if not sink_name:
            sink_port = self.get_output(sink_input)
        else:
            try:
                sink_port =\
                    self.get_processes()[sink_name].get_input(sink_input)
            except KeyError:
                raise ValueError("Invalid link {} -> {}".format(source, sink))

        link = ProcessLink(source_port, sink_port)
        self._links[source] = link

        # Tell our listeners a link has been created
        for l in self._workflow_events.listeners:
            l.link_created(self, source, sink)

        return link

    def get_links(self):
        return self._links

    def get_link(self, output):
        return self._links[output]

    def remove_link(self, output):
        link = self._links.pop(output)
        source = output
        sink = self.get_local_name(link.sink.process) + ":" + link.sink.name

        # Tell our listeners a link has been removed
        for l in self._workflow_events.listeners:
            l.link_removed(self, source, sink)

    def _run(self, **kwargs):
        for listener in self._workflow_events.listeners:
            listener.workflow_staring(self)

        out = self._run_workflow(**kwargs)

        for l in self._workflow_events.listeners:
            l.workflow_finished(self, out)

        return out

    def _run_workflow(self, **kwargs):
        for key, value in kwargs.iteritems():
            try:
                # Push the input value to the sink process input port
                self.get_link(":{}".format(key)).sink.push(value)
            except KeyError:
                # The input isn't connected, nae dramas
                pass

        to_run = self._get_ready_processes()
        while to_run:
            self._run_processes(to_run)
            to_run = self._get_ready_processes()

        # Now gather all my output values
        outputs = {}
        for name, port in self.get_outputs().iteritems():
            if port.is_filled():
                outputs[name] = port.pop()

        return outputs

    def get_local_name(self, process):
        for name, proc in self.get_processes().iteritems():
            if process == proc:
                return name
        raise ValueError("Process not in workflow")

    def _get_ready_processes(self):
        return filter(lambda proc: proc.ready_to_run(), self.get_processes().itervalues())

    def _run_processes(self, processes):
        """
        Consume all the processes, running them in turn and propagating their
        outputs to any connected processes.

        :param processes: A list of processes to run (from last to first)
        """
        while processes:
            process = processes.pop()
            outputs = process.run()
            local_name = self.get_local_name(process)
            for output_name, value in outputs.iteritems():
                source = local_name + ":" + output_name
                try:
                    sink_port = self.get_link(source).sink
                    sink_port.push(value)
                    sink_process = self.get_local_name(sink_port.process)\
                        if sink_port.process != self else ""
                    sink = "{}:{}".format(sink_process, sink_port.name)
                except KeyError:
                    # The output isn't connected, nae dramas
                    sink = None
                # Tell our listeners about the value being emitted
                for l in self._workflow_events.listeners:
                    l.value_outputted(self, value, source, sink)
