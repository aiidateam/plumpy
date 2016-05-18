# -*- coding: utf-8 -*-

from collections import namedtuple
from plum.persistence.process_record import ProcessRecord
from plum.persistence.persistence_manager import PersistenceManager
from plum.util import load_class
import pickle
from datetime import datetime
import tempfile
import os
import glob


_STORE_DIRECTORY = os.path.join(tempfile.gettempdir(), "process_records")


class FileProcessRecord(ProcessRecord):
    Checkpoint = namedtuple('Checkpoint', ['process_state', 'wait_on_state'])

    PROC_INSTANCE_STATE = 'proc_instance_state'
    WAIT_ON_INSTANCE_STATE = 'wait_on_instance_state'

    @classmethod
    def load(cls, fileobj):
        assert (not fileobj.closed)
        return pickle.load(fileobj)

    @classmethod
    def load_all(cls):
        process_records = []
        for filename in glob.glob(os.path.join(_STORE_DIRECTORY, "*.proc")):
            with open(filename, 'rb') as f:
                proc = cls.load(f)
                if proc:
                    process_records.append(proc)
        return process_records

    def __init__(self, process, inputs, pid, parent=None):
        self._pid = pid
        self._process_class = process.__module__ + "." + process.__class__.__name__
        self._inputs = inputs
        self._filename = "{}.proc".format(self.pid)
        self._last_saved = None
        self._checkpoint = None
        self._children = {}
        self._parent = parent

    @property
    def filename(self):
        return self._filename

    @property
    def pid(self):
        return self._pid

    @property
    def process_class(self):
        return self._process_class

    @property
    def inputs(self):
        return self._inputs

    @property
    def last_saved(self):
        return self._last_saved

    def set_checkpoint(self, checkpoint):
        self._checkpoint = checkpoint

    def create_checkpoint(self, process, wait_on=None):
        proc_state = {}
        process.save_instance_state(proc_state)
        wait_on_state = {}
        if wait_on:
            wait_on.save_instance_state(wait_on_state)
        self._checkpoint = self.Checkpoint(proc_state, wait_on_state)

    def save(self):
        if self._parent:
            self._parent.save()
        else:
            if not os.path.exists(_STORE_DIRECTORY):
                os.makedirs(_STORE_DIRECTORY)

            with open(os.path.join(_STORE_DIRECTORY, self._filename), 'wb') as f:
                # Gather all the things we want to store in order
                self._last_saved = datetime.now()
                pickle.dump(self, f)

    def delete(self):
        if self._parent:
            self._parent.remove_child(self._pid)
            # Need to save, otherwise we could get a stray child left on disk
            self._parent.save()
            self._parent = None
        else:
            # Write myself to disk
            try:
                os.remove(os.path.join(_STORE_DIRECTORY, self._filename))
            except OSError:
                pass

    def create_process(self):
        proc = load_class(self._process_class).create()
        try:
            if self._checkpoint:
                proc.load_instance_state(self._checkpoint.process_state)
        except KeyError:
            pass
        return proc

    def create_wait_on(self, exec_engine):
        WaitOn = load_class(self._checkpoint._wait_on_class)
        return WaitOn.create_from(self._checkpoint._wait_on_state, exec_engine)


class FilePersistenceManager(PersistenceManager):
    def __init__(self):
        super(FilePersistenceManager, self).__init__()
        self._records = {}

    def create_running_process_record(self, process, inputs, pid):
        record = FileProcessRecord(process, inputs, pid)
        self._records[pid] = record
        return record

    def get_record(self, pid):
        return self._records[pid]

    def delete_record(self, pid):
        self._records[pid].delete()
        del self._records[pid]

