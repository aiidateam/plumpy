from plum.knowledge_provider import KnowledgeProvider, NotKnown
from plum.util import override


class KnowledgeBase(KnowledgeProvider):
    def __init__(self):
        self._providers = []

    def add_provider(self, provider):
        """
        Add a knowledge provider to the knowledge base.  These will be checked
        in the order they were added.

        :param provider: The provider to add
        :type provider: :class:`KnowledgeProvider`
        """
        assert provider is not self
        self._providers.append(provider)

    def remove_provider(self, provider):
        """
        Remove a knowledge provider from the knowledge base.
        :param provider:  The provider to remove
        :type param: :class:`KnowledgeProvider`
        """
        self._providers.remove(provider)

    @override
    def get_input(self, pid, port_name):
        for p in self._providers:
            try:
                return p.get_input(pid, port_name)
            except ValueError:
                pass
        raise ValueError()

    @override
    def get_inputs(self, pid):
        for p in self._providers:
            try:
                return p.get_inputs(pid)
            except ValueError:
                pass
        raise ValueError()

    @override
    def get_output(self, pid, port_name):
        for p in self._providers:
            try:
                return p.get_output(pid, port_name)
            except ValueError:
                pass
        raise ValueError()

    @override
    def get_outputs(self, pid):
        for p in self._providers:
            try:
                return p.get_outputs(pid)
            except ValueError:
                pass
        raise ValueError()

    @override
    def has_finished(self, pid):
        for p in self._providers:
            try:
                return p.has_finished(pid)
            except ValueError:
                pass
        raise ValueError()

    @override
    def get_pids_from_classname(self, classname):
        all_pids = []
        for p in self._providers:
            try:
                all_pids.extend(p.get_pids_from_classname(classname))
            except ValueError:
                pass

        if all_pids:
            return all_pids
        else:
            raise ValueError()
