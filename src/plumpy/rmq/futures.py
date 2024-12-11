# -*- coding: utf-8 -*-
"""
Module containing future related methods and classes
"""

import kiwipy

__all__ = ['chain', 'copy_future', 'unwrap_kiwi_future']

copy_future = kiwipy.copy_future
chain = kiwipy.chain


def unwrap_kiwi_future(future: kiwipy.Future) -> kiwipy.Future:
    """
    Create a kiwi future that represents the final results of a nested series of futures,
    meaning that if the futures provided itself resolves to a future the returned
    future will not resolve to a value until the final chain of futures is not a future
    but a concrete value.  If at any point in the chain a future resolves to an exception
    then the returned future will also resolve to that exception.

    :param future: the future to unwrap
    :return: the unwrapping future

    """
    unwrapping = kiwipy.Future()

    def unwrap(fut: kiwipy.Future) -> None:
        if fut.cancelled():
            unwrapping.cancel()
        else:
            with kiwipy.capture_exceptions(unwrapping):
                result = fut.result()
                if isinstance(result, kiwipy.Future):
                    result.add_done_callback(unwrap)
                else:
                    unwrapping.set_result(result)

    future.add_done_callback(unwrap)
    return unwrapping
