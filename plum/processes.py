

from plum.process import Process
from plum.util import override
from plum.wait import WaitOn
from plum.exceptions import Unsupported
from plum.wait_ons import WaitOnAll, WaitOnProcess
from plum.persistence.bundle import Bundle


class ProcessWithChildren(Process):
    CHILDREN = 'children'

    class WaitOnChildren(WaitOn):
        @override
        def __init__(self, processes):
            super(ProcessWithChildren.WaitOnChildren, self).__init__()
            self._processes = processes

        @override
        def load_instance_state(self, bundle):
            raise Unsupported(
                "This wait on can only be created by the parent process, "
                "it cannot be instantiated on its own")
    
    def __init__(self):
        super(ProcessWithChildren, self).__init__()
        self._child_procs = []

    def add_child(self, process):
        self._child_procs.append(process)

    def remove_child(self, process):
        self._child_procs.remove(process)

    def get_children(self):
        return self._child_procs

    @override
    def pause(self):
        """
        Pause this process and all its children.
        """
        for child in self.get_children():
            child.pause()
        super(ProcessWithChildren, self).pause()

    @override
    def on_abort(self):
        """
        Make sure to abort all our children as well
        """
        super(ProcessWithChildren, self).on_abort()
        for child in self.get_children():
            child.abort(self.get_abort_msg())

    @override
    def save_instance_state(self, bundle):
        super(ProcessWithChildren, self).save_instance_state(bundle)
        child_states = []
        for child in self._child_procs:
            b = Bundle()
            child.save_instance_state(b)
            child_states.append(b)
        bundle[self.CHILDREN] = child_states

    @override
    def save_wait_on_state(self):
        # Nothing to save, we'll recreate it later
        return Bundle()

    @override
    def load_instance_state(self, bundle):
        super(ProcessWithChildren, self).load_instance_state(bundle)

        for child_state in bundle[self.CHILDREN]:
            self.add_child(
                Process.create_from(child_state, logger=self.logger)
            )

    @override
    def create_wait_on(self, bundle):
        return WaitOnAll([WaitOnProcess(p) for p in self._child_procs])
