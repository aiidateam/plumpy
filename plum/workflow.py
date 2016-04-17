# -*- coding: utf-8 -*-


"""
TODO:
* Make sure if there is an input port linked to multiple Processes then the
value is passed to all of them when the workflow starts.
"""

from abc import ABCMeta
import threading
from plum.port import DynamicOutputPort
from plum.process import Process, ProcessSpec, ProcessListener
import plum.util as util


class ProcessLink(object):
    def __init__(self, source, sink):
        self._source_process, self._source_port = source.split(':')
        self._sink_process, self._sink_port = sink.split(':')
        self._value = None

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
        return "{}:{} => {}:{}".format(
            self.source_process, self.source_port,
            self.sink_process, self.sink_port)


class WorkflowListener(object):
    __metaclass__ = ABCMeta

    def on_workflow_starting(self, workflow):
        """
        Called when a workflow is about to start, the next thing that happens
        will be that the workflow run method gets called.

        :param workflow: The workflow that is starting
        """
        pass

    def on_workflow_finished(self, workflow, outputs):
        """
        Called when a workflow is has finished.  All outputs have been checked
        and the workflow returned.

        :param workflow: The finished workflow
        :param outputs: The outputs from the workflow
        :return:
        """
        pass

    def on_subprocess_starting(self, workflow, subproc, inputs):
        """
        Called when the inputs of a subprocess passed checks and the process
        is about to begin.
        :param workflow: The workflow whose subprocess is starting
        :param subproc: The starting subprocess
        :param inputs: The inputs the subprocess is starting with
        """
        pass

    def on_subprocess_finalising(self, workflow, subproc):
        """
        Called when the subprocess has completed execution, however this may be
        the result of returning or an exception being raised.  Either way this
        message is guaranteed to be sent.  Only upon successful return and
        outputs passing checks would _on_process_finished be called.
        """
        pass

    def on_subprocess_finished(self, workflow, subproc, retval):
        """
        Called when the process has finished and the outputs have passed
        checks
        :param retval: The return value from the process
        """
        pass

    def on_value_buffered(self, workflow, link, value):
        """
        Called when an output value was emitted by a subprocess but the
        connected process is not ready to start yet because it is waiting
        for other values.
        :param link: The link connecting the source port to the sink.
        :param value: The value that was emitted
        """


class WorkflowSpec(ProcessSpec):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(WorkflowSpec, self).__init__()
        self._processes = {}
        self._outgoing_links = {}
        self._incoming_links = {}

    def process(self, process, local_name=None):
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

    def exposed_inputs(self, process_name):
        proc = self.get_process(process_name)
        for name, port in proc.spec().inputs.iteritems():
            self.input_port(name, port)
            self.link(":{}".format(name),
                      "{}:{}".format(process_name, name))

    def exposed_outputs(self, process_name):
        proc = self.get_process(process_name)
        for name, port in proc.spec().outputs.iteritems():
            self.output_port(name, port)
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
        self._input_buffer = {}
        self._num_running_subprocs = util.ThreadSafeCounter()
        self._wait_event = None

        # Create all our subprocess classes
        for name, proc_class in self.spec().processes.iteritems():
            proc = proc_class.create()
            proc.add_process_listener(self)
            self._process_instances[name] = proc

    # From ProcessListener ##########################
    def on_process_starting(self, process, inputs):
        self._on_subprocess_starting(process, inputs)

    def on_output_emitted(self, process, output_port, value, dynamic):
        local_name = self.get_local_name(process)
        if dynamic:
            source = local_name + ":" + DynamicOutputPort.NAME
        else:
            source = local_name + ":" + output_port
        try:
            link = self.spec().get_link(source)
            if not link.sink_process:
                self._out(link.sink_port, value)
            else:
                proc = self._process_instances[link.sink_process]
                self._push_value(link, value)

                # If the process receiving this input is ready then run it
                try:
                    inputs = self._generate_inputs_for(proc)
                    self._launch_subprocess(proc, inputs)
                except ValueError:
                    # Not ready to run
                    self._on_value_buffered(link, value)

            sink = "{}:{}".format(link.sink_process, link.sink_port)
        except KeyError:
            # The output isn't connected, nae dramas
            sink = None

    def on_process_finalising(self, process):
        self._on_subprocess_finalising(process)

    def on_process_finished(self, process, retval):
        self._on_subprocess_finished(process, retval)
    ##################################################

    def _run(self, **kwargs):
        self._workflow_evt_helper.fire_event(
            WorkflowListener.on_workflow_starting, self)

        self._run_workflow(**kwargs)

        self._workflow_evt_helper.fire_event(
            WorkflowListener.on_workflow_finished, self)

    def _run_workflow(self, **kwargs):
        self._initialise_inputs(**kwargs)

        self._wait_event = threading.Event()

        for proc in self._process_instances.itervalues():
            try:
                inputs = self._generate_inputs_for(proc)
                self._launch_subprocess(proc, inputs)
            except ValueError:
                pass

        # Wait for all subprocess to finish
        self._wait_event.wait()
        self._wait_event = None

    def get_local_name(self, process):
        for name, proc in self._process_instances.iteritems():
            if process == proc:
                return name
        raise ValueError("Process not in workflow")

    def _initialise_inputs(self, **kwargs):
        for key, value in kwargs.iteritems():
            try:
                # Push the input value to the links
                link = self.spec().get_link(":{}".format(key))
                self._push_value(link, value)
            except KeyError:
                # The input isn't connected, nae dramas
                pass

    def _generate_inputs_for(self, proc):
        ready_links = []
        for input_name in proc.spec().inputs:
            sink = "{}:{}".format(self.get_local_name(proc), input_name)
            for link in self.spec().get_incoming_links(sink):
                if str(link) in self._input_buffer:
                    ready_links.append(link)
                else:
                    raise ValueError(
                        "Cannot generate inputs for process as not all links "
                        "have a value ready.")
        # All the links have ready values so get them
        return {link.sink_port: self._pop_value(link) for link in ready_links}

    def _push_value(self, link, value):
        self._input_buffer[str(link)] = value

    def _pop_value(self, link):
        return self._input_buffer.pop(str(link))

    def _launch_subprocess(self, proc, inputs):
        self._num_running_subprocs.increment()
        self._get_exec_engine().submit(proc, inputs).add_done_callback(self._subproc_done)

    def _subproc_done(self, fut):
        self._num_running_subprocs.decrement()
        if self._num_running_subprocs.value == 0:
            self._wait_event.set()

    # Workflow messages #################################################
    # Make sure to call the superclass if your override any of these ####
    def _on_subprocess_starting(self, subproc, inputs):
        """
        Called when the inputs of a subprocess passed checks and the process
        is about to begin.

        :param subproc: The subprocess that is starting
        :param inputs: The inputs the process is starting with
        """
        self._workflow_evt_helper.fire_event('on_subprocess_starting',
                                             self, subproc, inputs)

    def _on_subprocess_finalising(self, subproc):
        """
        Called when the subprocess has completed execution, however this may be
        the result of returning or an exception being raised.  Either way this
        message is guaranteed to be sent.  Only upon successful return and
        outputs passing checks would _on_process_finished be called.

        :param subproc: The subprocess that is finalising
        """
        self._workflow_evt_helper.fire_event('on_subprocess_finalising',
                                             self, subproc)

    def _on_subprocess_finished(self, subproc, retval):
        """
        Called when the process has finished and the outputs have passed
        checks
        :param subproc: The subprocess that has finished
        :param retval: The return value from the process
        """
        self._workflow_evt_helper.fire_event('on_subprocess_finished',
                                             self, subproc, retval)

    def _on_value_buffered(self, link, value):
        """
        Called when an output value was emitted by a subprocess but the
        connected process is not ready to start yet because it is waiting
        for other values.
        :param link: The link connecting the source port to the sink.
        :param value: The value that was emitted
        """
        self._workflow_evt_helper.fire_event('on_value_buffered',
                                             self, link, value)

    #####################################################################
