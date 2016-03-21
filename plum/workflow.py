# -*- coding: utf-8 -*-


"""
TODO:
* Make sure if there is an input port linked to multiple Processes then the
value is passed to all of them when the workflow starts.
"""

from abc import ABCMeta
from plum.process import Process, ProcessSpec, ProcessListener
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


class WorkflowListener(object):
    __metaclass__ = ABCMeta

    def on_workflow_starting(self, workflow):
        pass

    def on_workflow_finished(self, workflow, outputs):
        pass


class WorkflowSpec(ProcessSpec):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(WorkflowSpec, self).__init__()
        self._processes = {}
        self._outgoing_links = {}
        self._incoming_links = {}

    def add_process(self, process, local_name=None):
        name = local_name if local_name else process.get_name()
        self._processes[name] = process

    def remove_process(self, local_name):
        # TODO: Check for links and remove as appropriate
        self._processes.pop(local_name)

    @property
    def processes(self):
        return self._processes

    def get_process(self, name):
        return self._processes[name]

    def expose_inputs(self, process_name):
        proc = self.get_process(process_name)
        for name, port in proc.spec().inputs.iteritems():
            self.add_input_port(name, port)
            self.link(":{}".format(name),
                      "{}:{}".format(process_name, name))

    def expose_outputs(self, process_name):
        proc = self.get_process(process_name)
        for name, port in proc.spec().outputs.iteritems():
            self.add_output_port(name, port)
            self.link("{}:{}".format(process_name, name),
                      ":{}".format(name))

    @property
    def links(self):
        return self._outgoing_links

    @property
    def incoming_links(self):
        return self._incoming_links

    def get_incoming_links(self, sink):
        if sink in self._incoming_links:
            return self._incoming_links[sink]
        else:
            return []

    def get_link(self, output):
        return self._outgoing_links[output]

    def remove_link(self, output):
        self._outgoing_links.pop(output)

    def link(self, source, sink):
        if source in self.links:
            raise ValueError("Link from {} already exists, remove first")

        link = ProcessLink(source, sink)

        if not link.source_process:
            # Use workflow input port
            source_port = self.get_input(link.source_port)
        else:
            try:
                source_port = \
                    self._processes[link.source_process].spec().get_output(link.source_port)
            except KeyError:
                raise ValueError("Invalid link {} -> {}".format(source, sink))

        if not link.sink_process:
            sink_port = self.get_output(link.sink_port)
        else:
            try:
                sink_port = \
                    self._processes[link.sink_process].spec().get_input(link.sink_port)
            except KeyError:
                raise ValueError("Invalid link {} -> {}".format(source, sink))

        # TODO: Check type compatibility of source and sink

        self._outgoing_links[source] = link
        self._incoming_links.setdefault(sink, []).append(link)
        return link


class Workflow(Process, ProcessListener):
    __metaclass__ = ABCMeta

    # Static class stuff ######################
    _spec_type = WorkflowSpec

    ###########################################

    def __init__(self):
        super(Workflow, self).__init__()
        self._workflow_evt_helper = util.EventHelper(WorkflowListener)

        self._process_instances = {}
        for name, proc_class in self.spec().processes.iteritems():
            proc = proc_class.create()
            proc.add_process_listener(self)
            self._process_instances[name] = proc

    # From ProcessListener ##########################
    def on_output_emitted(self, process, output_port, value):
        local_name = self.get_local_name(process)
        source = local_name + ":" + output_port
        try:
            link = self.spec().get_link(source)
            if not link.sink_process:
                self._out(link.sink_port, value)
            else:
                proc = self._process_instances[link.sink_process]
                proc.bind(link.sink_port, value)

                # If the process receiving this input is ready then run it
                if self._is_ready_to_run(proc):
                    proc.run()

            sink = "{}:{}".format(link.sink_process, link.sink_port)
        except KeyError:
            # The output isn't connected, nae dramas
            sink = None

    ##################################################

    def _run(self, **kwargs):
        self._workflow_evt_helper.fire_event(
            WorkflowListener.on_workflow_starting, self)

        self._run_workflow(**kwargs)

        self._workflow_evt_helper.fire_event(
            WorkflowListener.on_workflow_finished, self)

    def _run_workflow(self, **kwargs):
        self._initialise_inputs(**kwargs)

        for proc in self._get_ready_processes():
            proc.run()

    def _save_record(self, node):
        pass

    def get_local_name(self, process):
        for name, proc in self._process_instances.iteritems():
            if process == proc:
                return name
        raise ValueError("Process not in workflow")

    def _get_ready_processes(self):
        return filter(lambda proc: self._is_ready_to_run(proc),
                      self._process_instances.itervalues())

    def _is_ready_to_run(self, proc):
        # Check that any connected ports are filled, otherwise it's not ready
        for input_name in proc.spec().inputs:
            sink = "{}:{}".format(self.get_local_name(proc), input_name)
            if (self.spec().get_incoming_links(sink) and
                    not proc.is_input_bound(input_name)):
                return False
        return True

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
