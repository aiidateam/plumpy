# -*- coding: utf-8 -*-
import time
from typing import final

from plumpy.base import state_machine
from plumpy.exceptions import InvalidStateError
import pytest

# Events
PLAY = 'Play'
PAUSE = 'Pause'
STOP = 'Stop'

# States
PLAYING = 'Playing'
PAUSED = 'Paused'
STOPPED = 'Stopped'


class Playing:
    LABEL = PLAYING
    ALLOWED = {PAUSED, STOPPED}
    TRANSITIONS = {STOP: STOPPED}

    is_terminal = False

    def __init__(self, player, track):
        assert track is not None, 'Must provide a track name'
        self.track = track
        self._last_time = None
        self._played = 0.0
        self.in_state = False

    def __str__(self):
        if self.in_state:
            self._update_time()
        return f'> {self.track} ({self._played}s)'

    def play(self, track=None):
        return False

    def _update_time(self):
        current_time = time.time()
        self._played += current_time - self._last_time
        self._last_time = current_time

    def enter(self) -> None:
        self._last_time = time.time()
        self.in_state = True

    def exit(self) -> None:
        if self.is_terminal:
            raise InvalidStateError(f'Cannot exit a terminal state {self.LABEL}')

        self._update_time()
        self.in_state = False


class Paused:
    LABEL = PAUSED
    ALLOWED = {PLAYING, STOPPED}
    TRANSITIONS = {STOP: STOPPED}

    is_terminal = False

    def __init__(self, player, playing_state):
        assert isinstance(playing_state, Playing), 'Must provide the playing state to pause'
        self._player = player
        self.playing_state = playing_state

    def __str__(self):
        return f'|| ({self.playing_state})'

    def play(self, track=None):
        if track is not None:
            self._player.transition_to(Playing(player=self.state_machine, track=track))
        else:
            self._player.transition_to(self.playing_state)

    def enter(self) -> None:
        self.in_state = True

    def exit(self) -> None:
        if self.is_terminal:
            raise InvalidStateError(f'Cannot exit a terminal state {self.LABEL}')

        self.in_state = False


class Stopped:
    LABEL = STOPPED
    ALLOWED = {
        PLAYING,
    }
    TRANSITIONS = {PLAY: PLAYING}

    is_terminal = False

    def __init__(self, player):
        self._player = player

    def __str__(self):
        return '[]'

    def play(self, track):
        self._player.transition_to(Playing(self._player, track=track))

    def enter(self) -> None:
        self.in_state = True

    def exit(self) -> None:
        if self.is_terminal:
            raise InvalidStateError(f'Cannot exit a terminal state {self.LABEL}')

        self.in_state = False


class CdPlayer(state_machine.StateMachine):
    STATES = (Stopped, Playing, Paused)

    def __init__(self):
        super().__init__()
        self.add_state_event_callback(
            state_machine.StateEventHook.ENTERING_STATE, lambda _s, _h, state: self.entering(state)
        )
        self.add_state_event_callback(state_machine.StateEventHook.EXITING_STATE, lambda _s, _h, _st: self.exiting())

    def entering(self, state):
        print(f'Entering {state}')
        print(self._state)

    def exiting(self):
        print(f'Exiting {self.state}')
        print(self._state)

    @state_machine.event(to_states=Playing)
    def play(self, track=None):
        return self._state.play(track)

    @state_machine.event(from_states=Playing, to_states=Paused)
    def pause(self):
        self.transition_to(Paused(self, playing_state=self._state))
        return True

    @state_machine.event(from_states=(Playing, Paused), to_states=Stopped)
    def stop(self):
        self.transition_to(Stopped(self))


class TestStateMachine:
    def test_basic(self):
        cd_player = CdPlayer()
        assert cd_player.state_label == STOPPED

        cd_player.play('Eminem - The Real Slim Shady')
        assert cd_player.state_label == PLAYING
        time.sleep(1.0)

        cd_player.pause()
        assert cd_player.state_label == PAUSED

        cd_player.play()
        assert cd_player.state_label == PLAYING

        assert cd_player.play() == False

        cd_player.stop()
        assert cd_player.state_label == STOPPED

    def test_invalid_event(self):
        cd_player = CdPlayer()
        with pytest.raises(AssertionError):
            cd_player.play()
