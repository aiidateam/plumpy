from kiwipy import rmq
from tornado import testing

import plumpy
from plumpy import communications, test_utils, process_comms


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


class TestProcessLauncher(testing.AsyncTestCase):
    def setUp(self):
        super(TestProcessLauncher, self).setUp()
        self.loop = self.io_loop

    @testing.gen_test
    def test_continue(self):
        persister = plumpy.InMemoryPersister()
        load_context = plumpy.LoadSaveContext(loop=self.loop)
        launcher = plumpy.ProcessLauncher(persister=persister, load_context=load_context)

        process = test_utils.DummyProcess(loop=self.loop)
        pid = process.pid
        persister.save_checkpoint(process)
        del process
        process = None

        result = yield launcher._continue(None, **plumpy.create_continue_body(pid)[process_comms.TASK_ARGS])
        self.assertEqual(test_utils.DummyProcess.EXPECTED_OUTPUTS, result)

    @testing.gen_test
    def test_loader_is_used(self):
        """ Make sure that the provided class loader is used by the process launcher """
        loader = CustomObjectLoader()
        proc = Process()
        persister = plumpy.InMemoryPersister(loader=loader)
        persister.save_checkpoint(proc)
        launcher = plumpy.ProcessLauncher(persister=persister, loader=loader)

        continue_task = plumpy.create_continue_body(proc.pid)
        yield launcher._continue(None, **continue_task[process_comms.TASK_ARGS])
