import unittest
import plumpy
from plumpy import test_utils


class Process(plumpy.Process):
    def run(self):
        pass


class CustomObjectLoader(plumpy.DefaultObjectLoader):
    def load_object(self, identifier):
        if identifier == "jimmy":
            return Process
        else:
            return super(CustomObjectLoader, self).load_object(identifier)

    def identify_object(self, obj):
        if isinstance(obj, Process) or issubclass(obj, Process):
            return "jimmy"
        else:
            return super(CustomObjectLoader, self).identify_object(obj)


class TestProcessLauncher(unittest.TestCase):
    def test_continue(self):
        loop = plumpy.new_event_loop()
        persister = plumpy.InMemoryPersister()
        load_context = plumpy.LoadSaveContext(loop=loop)
        launcher = plumpy.ProcessLauncher(persister=persister, load_context=load_context)

        process = test_utils.DummyProcess()
        persister.save_checkpoint(process)

        future = launcher._continue(plumpy.create_continue_body(process.pid))
        result = loop.run_sync(lambda: future)

    def test_loader_is_used(self):
        """ Make sure that the provided class loader is used by the process launcher """
        loader = CustomObjectLoader()
        proc = Process()
        persister = plumpy.InMemoryPersister(loader=loader)
        persister.save_checkpoint(proc)
        launcher = plumpy.ProcessLauncher(persister=persister, loader=loader)
        continue_task = plumpy.create_continue_body(proc.pid)
        launcher._continue(continue_task)
