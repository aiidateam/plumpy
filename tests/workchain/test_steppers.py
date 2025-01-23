# -*- coding: utf-8 -*-
import pytest
from plumpy.base.state_machine import StateMachine
from plumpy.persistence import LoadSaveContext, Savable, load
from plumpy.workchains import WorkChain, if_, while_


class DummyWc(WorkChain):
    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.outline(
            cls.do_nothing,
            if_(cls.cond)(cls.do_cond),
            while_(cls.wcond)(
                cls.do_wcond,
            ),
        )

    @staticmethod
    def do_nothing(_wc: WorkChain) -> None:
        pass

    @staticmethod
    def cond(_wc: WorkChain) -> bool:
        return True

    @staticmethod
    def do_cond(_wc: WorkChain) -> None:
        pass

    @staticmethod
    def wcond(_wc: WorkChain) -> bool:
        return True

    @staticmethod
    def do_wcond(_wc: WorkChain) -> None:
        pass


@pytest.fixture(scope='function')
def wc() -> StateMachine:
    return DummyWc()


def test_func_stepper_savable(wc: DummyWc):
    from plumpy.workchains import _FunctionStepper

    fs = _FunctionStepper(workchain=wc, fn=wc.do_nothing)
    assert isinstance(fs, Savable)

    ctx = LoadSaveContext(workchain=wc)
    saved_state = fs.save()
    loaded_state = load(saved_state=saved_state, load_context=ctx)
    saved_state2 = loaded_state.save()

    assert saved_state == saved_state2


def test_block_stepper_savable(wc: DummyWc):
    """block stepper test with a dummy function call"""
    from plumpy.workchains import _BlockStepper, _FunctionCall

    block = [_FunctionCall(wc.do_nothing)]
    bs = _BlockStepper(block=block, workchain=wc)
    assert isinstance(bs, Savable)

    ctx = LoadSaveContext(workchain=wc, block_instruction=block)
    saved_state = bs.save()
    loaded_state = load(saved_state=saved_state, load_context=ctx)
    saved_state2 = loaded_state.save()

    assert saved_state == saved_state2


def test_if_stepper_savable(wc: DummyWc):
    """block stepper test with a dummy function call"""
    from plumpy.workchains import _If, _IfStepper

    dummy_if = _If(wc.cond)
    ifs = _IfStepper(if_instruction=dummy_if, workchain=wc)
    assert isinstance(ifs, Savable)

    ctx = LoadSaveContext(workchain=wc, if_instruction=ifs)
    saved_state = ifs.save()
    loaded_state = load(saved_state=saved_state, load_context=ctx)
    saved_state2 = loaded_state.save()

    assert saved_state == saved_state2


def test_while_stepper_savable(wc: DummyWc):
    """block stepper test with a dummy function call"""
    from plumpy.workchains import _While, _WhileStepper

    dummy_while = _While(wc.cond)
    wfs = _WhileStepper(while_instruction=dummy_while, workchain=wc)
    assert isinstance(wfs, Savable)

    ctx = LoadSaveContext(workchain=wc, while_instruction=wfs)
    saved_state = wfs.save()
    loaded_state = load(saved_state=saved_state, load_context=ctx)
    saved_state2 = loaded_state.save()

    assert saved_state == saved_state2
