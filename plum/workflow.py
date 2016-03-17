# -*- coding: utf-8 -*-

from abc import ABCMeta
import plum.event as event
from plum.process import Process, ProcessSpec
import plum.util as util


class ProcessLink(object):
    def __init__(self, source, sink):
        self._source_process, self._source_port = source.split(':')
        self._sink_process, self._sink_port = sink.split(':')

    @property
    def source_process(self):
        return self._source_process

    @property
    def source_port(self):
        return self._source_port

    @property
    def sink_process(self):
        return self._sink_process

    @property
    def sink_port(self):
        return self._sink_port

    def __str__(self):
        return "{}:{} => {}:{}".format(self.source_process, self.source_port,
                                       self.sink_process, self.sink_port)


class WorkflowSpec(ProcessSpec):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(WorkflowSpec, self).__init__()
        self._processes = {}
        self._links = {}

    def add_process(self, process, local_name=None):
        name = local_name if local_name else process.get_name()
        self._processes[name] = process

    def remove_process(self, local_name):
        # TODO: Check for links and remove as appropriate
        self._processes.pop(local_name)

    @property
    def processes(self):
        return self._processes

    @property
    def links(self):
        return self._links

    def get_link(self, output):
        return self._links[output]

    def remove_link(self, output):
        self._links.pop(output)

    def link(self, source, sink):
        if source in self.links:
            raise ValueError("Link from {} already exists, remove first")

        link = ProcessLink(source, sink)

        if not link.source_process:
            # Use workflow input port
            source_port = self.get_input(link.source_port)
        else:
            try:
                source_port =\
                    self._processes[link.source_process].spec().get_output(link.source_port)
            except KeyError:
                raise ValueError("Invalid link {} -> {}".format(source, sink))

        if not link.sink_process:
            sink_port = self.get_output(link.sink_port)
        else:
            try:
                sink_port =\
                    self._processes[link.sink_process].spec().get_input(link.sink_port)
            except KeyError:
                raise ValueError("Invalid link {} -> {}".format(source, sink))

        # TODO: Check type compatibility of source and sink

        self._links[source] = link
        return link


class Workflow(Process):
    __metaclass__ = ABCMeta

    # Static class stuff ######################
    _spec_type = WorkflowSpec
    ###########################################

    def __init__(self):
        super(Workflow, self).__init__()
        self._workflow_events = util.EventHelper(event.WorkflowListener)

        self._process_instances =\
            {name: proc.create()
             for name, proc in self.spec().processes.iteritems()}

    # From ProcessListener ##########################
    def process_starting(self, process, inputs):
        # Tell our listeners that a subprocess is starting
        for l in self._workflow_events.listeners:
            l.subprocess_starting(self, process, inputs)

    def process_finished(self, process, outputs):
        # Tell our listeners that a subprocess has finished
        for l in self._workflow_events.listeners:
            l.subprocess_finished(self, process, outputs)
    ##################################################

    def _run(self, **kwargs):
        for listener in self._workflow_events.listeners:
            listener.workflow_staring(self)

        out = self._run_workflow(**kwargs)

        for l in self._workflow_events.listeners:
            l.workflow_finished(self, out)

        return out

    def _run_workflow(self, **kwargs):
        self._initialise_inputs(**kwargs)

        outputs = {}

        to_run = self._get_ready_processes()
        while to_run:
            outputs.update(self._run_processes(to_run))
            to_run = self._get_ready_processes()

        return outputs

    def _save_record(self, node):
        pass

    def get_local_name(self, process):
        for name, proc in self._process_instances.iteritems():
            if process == proc:
                return name
        raise ValueError("Process not in workflow")

    def _get_ready_processes(self):
        return filter(lambda proc: proc.ready_to_run(), self._process_instances.itervalues())

    def _initialise_inputs(self, **kwargs):
        for key, value in kwargs.iteritems():
            try:
                # Push the input value to the sink process input port
                link = self.spec().get_link(":{}".format(key))
                proc = self._process_instances[link.sink_process]
                proc.bind(link.sink_port, value)
            except KeyError:
                # The input isn't connected, nae dramas
                pass

    def _run_processes(self, processes):
        """
        Consume all the processes, running them in turn and propagating their
        outputs to any connected processes.

        :param processes: A list of processes to run (from last to first)
        """
        workflow_outputs = {}
        while processes:
            process = processes.pop()
            outputs = process.run()
            local_name = self.get_local_name(process)
            for output_name, value in outputs.iteritems():
                source = local_name + ":" + output_name
                try:
                    link = self.spec().get_link(source)
                    if not link.sink_process:
                        workflow_outputs[link.sink_port] = value
                    else:
                        proc = self._process_instances[link.sink_process]
                        proc.bind(link.sink_port, value)

                    sink = "{}:{}".format(link.sink_process, link.sink_port)
                except KeyError:
                    # The output isn't connected, nae dramas
                    sink = None
                # Tell our listeners about the value being emitted
                for l in self._workflow_events.listeners:
                    l.value_outputted(self, value, source, sink)

        return workflow_outputs


