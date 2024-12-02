# -*- coding: utf-8 -*-
from kiwipy import rmq

import plumpy


class WaitForResumeProc(plumpy.Process):
    def run(self):
        print(f'Now I am running: {self.state}')
        return plumpy.Wait(self.after_resume_and_exec)

    def after_resume_and_exec(self):
        print(f'After resume from waiting state: {self.state}')


kwargs = {
    'connection_params': {'url': 'amqp://guest:guest@127.0.0.1:5672/'},
    'message_exchange': 'WaitForResume.uuid-0',
    'task_exchange': 'WaitForResume.uuid-0',
    'task_queue': 'WaitForResume.uuid-0',
}

if __name__ == '__main__':
    with rmq.RmqThreadCommunicator.connect(**kwargs) as communicator:
        proc = WaitForResumeProc(communicator=communicator)
        process_controller = plumpy.RemoteProcessThreadController(communicator)

        status_future = process_controller.get_status(proc.pid)
        print(status_future.result())  # pause: False

        process_controller.pause_process(proc.pid)
        status_future = process_controller.get_status(proc.pid)
        print(status_future.result())  # pause: True

        process_controller.play_process(proc.pid)
        status_future = process_controller.get_status(proc.pid)
        print(status_future.result())  # pause: False
