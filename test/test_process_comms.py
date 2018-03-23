import unittest
import plumpy
from plumpy import test_utils
from plumpy import test_utils
from plumpy import process_comms


class Process(plumpy.Process):
    def run(self):
        pass


class ClassLoader(plumpy.ClassLoader):
    def load_class(self, identifier):
        if identifier == "jimmy":
            return Process
        else:
            return super(ClassLoader, self).load_class(identifier)

    def class_identifier(self, obj):
        if isinstance(obj, Process) or issubclass(obj, Process):
            return "jimmy"
        else:
            return super(ClassLoader, self).class_identifier(obj)


class TestProcessLauncher(unittest.TestCase):
    def test_continue(self):
        loop = plumpy.new_event_loop()
        persister = plumpy.InMemoryPersister()
        load_context = plumpy.LoadContext(loop=loop)
        launcher = plumpy.ProcessLauncher(persister=persister, load_context=load_context)

        process = test_utils.DummyProcess()
        persister.save_checkpoint(process)

        future = launcher._continue(plumpy.create_continue_body(process.pid))
        result = loop.run_sync(lambda: future)

    def test_class_loader_is_used(self):
        """ Make sure that the provided class loader is used by the process launcher """
        cl = ClassLoader()
        proc = Process()
        persister = plumpy.InMemoryPersister(class_loader_=cl)
        persister.save_checkpoint(proc)
        launcher = plumpy.ProcessLauncher(persister=persister, class_loader=cl)
        continue_task = plumpy.create_continue_body(proc.pid)
        launcher._continue(continue_task)
