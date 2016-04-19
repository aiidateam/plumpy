# -*- coding: utf-8 -*-

from plum.persistence.process_record import ProcessRecord
import pickle
from datetime import datetime
import tempfile
import os
import glob


_STORE_DIRECTORY = os.path.join(tempfile.gettempdir(), "process_records")


class FileProcessRecord(ProcessRecord):
    _num_processes = 0

    @classmethod
    def generate_id(cls):
        pid = cls._num_processes
        cls._num_processes += 1
        return pid

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

    def __init__(self, process, inputs, _id=None, parent=None):

        if _id:
            self._id = _id
        else:
            self._id = self.generate_id()
        self._process_class = process.__module__ + "." + process.__class__.__name__
        self._inputs = inputs
        self._filename = "{}.proc".format(self.pid)
        self._last_saved = None
        self._instance_state = {}
        self._children = {}
        self._parent = parent

    @property
    def pid(self):
        return self._id

    @property
    def process_class(self):
        return self._process_class

    @property
    def inputs(self):
        return self._inputs

    @property
    def last_saved(self):
        return self._last_saved

    @property
    def instance_state(self):
        return self._instance_state

    @property
    def children(self):
        return self._children

    def create_child(self, process, inputs):
        pid = self.generate_id()
        child = FileProcessRecord(process, inputs, _id=pid, parent=self)
        self._children[pid] = child
        return child

    def remove_child(self, pid):
        self._children.pop(pid)

    def has_child(self, pid):
        return pid in self._children

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
            self._parent.remove_child(self._id)
            # Need to save, otherwise we could get a stray child left on disk
            self._parent.save()
            self._parent = None
        else:
            # Write myself to disk
            try:
                os.remove(os.path.join(_STORE_DIRECTORY, self._filename))
            except OSError:
                pass

