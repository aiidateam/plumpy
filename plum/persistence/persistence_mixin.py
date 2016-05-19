# -*- coding: utf-8 -*-

from plum.process import Process
from plum.persistence.file_persistence import FileProcessRecord
from abc import ABCMeta, abstractmethod


class PersistenceMixin(object):
    """
    This mixin is to be used with the Process class (or subclasses of it) to
    enable persistence functionality whereby a process can save its progress
    in the form of a ProcessRecord which can be used to continue from a
    previously reached state.
    """

    __metaclass__ = ABCMeta

    class ContinueScope(object):
        """
        A context manager to be used as:

        with ContinueScope(...):
          self._continue_from()

        It defines the scope of a process execution and produces the internal
        event messages at the end of the scope as well as other
        internal process management.
        """
        def __init__(self, process, record, exec_engine):
            self._process = process
            self._record = record
            self._exec_engine = exec_engine

        def __enter__(self):
            self._process._on_process_continuing(self._record)
            self._process._exec_engine = self._exec_engine

        def __exit__(self, type, value, traceback):
            self._process._exec_engine = None
            self._process.on_finalise()

    def __init__(self):
        assert isinstance(self, Process),\
            "This mixin has to be used with Process classes"
        super(PersistenceMixin, self).__init__()

    def continue_from(self, record, exec_engine=None):
        if not exec_engine:
            exec_engine = self._create_default_exec_engine()
        with self.ContinueScope(self, record, exec_engine):
            self._continue_from(record)

        self._check_outputs()
        self._on_process_finished(None)

    def _create_process_record(self, inputs):
        return FileProcessRecord(self, inputs)

    def _on_process_starting(self, inputs):
        super(PersistenceMixin, self)._on_process_starting(inputs)

        self._process_record = self._create_process_record(inputs)
        self._process_record.save()

    def _on_process_continuing(self, record):
        self._output_values = {}
        self._process_record = record

    def _on_process_finalising(self):
        super(PersistenceMixin, self)._on_process_finalising()

        self._process_record.delete()
        self._process_record = None

    @abstractmethod
    def _continue_from(self, record):
        pass
