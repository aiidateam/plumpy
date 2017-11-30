from enum import Enum
import unittest
from plum.base import statemachine
import time

# Events
PLAY = 'Play'
PAUSE = 'Pause'
STOP = 'Stop'

# States
PLAYING = 'Playing'
PAUSED = 'Paused'
STOPPED = 'Stopped'


class Playing(statemachine.State):
    LABEL = PLAYING
    ALLOWED = {PAUSED, STOPPED}
    TRANSITIONS = {
        STOP: STOPPED
    }

    def __init__(self, state_machine, track):
        assert track is not None, "Must provide a track name"
        super(Playing, self).__init__(state_machine)
        self.track = track
        self._last_time = None
        self._played = 0.

    def __str__(self):
        if self.in_state:
            self._update_time()
        return "> {} ({}s)".format(self.track, self._played)

    def enter(self):
        super(Playing, self).enter()
        self._last_time = time.time()

    def exit(self):
        super(Playing, self).exit()
        self._update_time()

    def evt(self, event, *args, **kwargs):
        if event == PAUSE:
            self.transition_to(PAUSED, playing_state=self)
        else:
            super(Playing, self).evt(event, *args, **kwargs)

    def _update_time(self):
        current_time = time.time()
        self._played += current_time - self._last_time
        self._last_time = current_time


class Paused(statemachine.State):
    LABEL = PAUSED
    ALLOWED = {PLAYING, STOPPED}
    TRANSITIONS = {
        STOP: STOPPED
    }

    def __init__(self, state_machine, playing_state):
        assert isinstance(
            playing_state,
            state_machine.get_state_class(PLAYING)), \
            "Must provide the playing state to pause"
        super(Paused, self).__init__(state_machine)
        self.playing_state = playing_state

    def __str__(self):
        return "|| ({})".format(self.playing_state)

    def evt(self, event, *args, **kwargs):
        if event == PLAY:
            self.transition_to(self.playing_state)
        else:
            super(Paused, self).evt(event, *args, **kwargs)


class Stopped(statemachine.State):
    LABEL = STOPPED
    ALLOWED = {PLAYING, }
    TRANSITIONS = {
        PLAY: PLAYING
    }

    def __str__(self):
        return "[]"


class CdPlayer(statemachine.StateMachine):
    STATES = (Stopped, Playing, Paused)

    def on_entered(self):
        print("Entered {}".format(self.state))
        print(self._state)

    def on_exiting(self):
        print("Exiting {}".format(self.state))
        print(self._state)


class TestStateMachine(unittest.TestCase):
    def test_basic(self):
        cd_player = CdPlayer()
        self.assertEqual(cd_player.state, STOPPED)

        cd_player.evt(PLAY, 'Eminem - The Real Slim Shady')
        self.assertEqual(cd_player.state, PLAYING)
        time.sleep(1.)

        cd_player.evt(PAUSE)
        self.assertEqual(cd_player.state, PAUSED)

        cd_player.evt(PLAY)
        self.assertEqual(cd_player.state, PLAYING)

        cd_player.evt(STOP)
        self.assertEqual(cd_player.state, STOPPED)
