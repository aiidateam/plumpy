import unittest
from plum.loop import persistence
from plum import loop_factory


class TestContextMixin(unittest.TestCase):
    def setUp(self):
        super(TestContextMixin, self).setUp()
        self.loop = loop_factory()

    def test_non_persistable(self):
        """
        Try to use the mixin not with a persistable.
        """

        class WithContext(persistence.ContextMixin):
            pass

        with self.assertRaises(TypeError):
            WithContext(None)

    def test_simple(self):
        class WithContext(persistence.ContextMixin, persistence.PersistableLoopObject):
            pass

        # Create object with context
        loop_obj = self.loop.create(WithContext)

        # Populate the context
        loop_obj.ctx.a = 5
        loop_obj.ctx.b = ('a', 'b')

        # Persist the object in a bundle
        saved_state = persistence.Bundle()
        loop_obj.save_instance_state(saved_state)

        # Have to remove the original (because UUIDs are same)
        self.loop.remove(loop_obj)

        # Load the object from the saved state and compare contexts
        loaded_loop_obj = self.loop.create(WithContext, saved_state)
        self.assertEqual(loop_obj.ctx, loaded_loop_obj.ctx)
