# -*- coding: utf-8 -*-
from kiwipy import rmq
import plumpy


class WaitForResumeProc(plumpy.Process):

    def run(self):
        print('Now I am running: {:}'.format(self.state))
        return plumpy.Wait(self.after_resume_and_exec)

    def after_resume_and_exec(self):
        print('After resume from waiting state: {:}'.format(self.state))


def launch():
    message_exchange = '{}.{}'.format('WaitForResume', 'uuid-0')
    task_exchange = '{}.{}'.format('WaitForResume', 'uuid-0')
    task_queue = '{}.{}'.format('WaitForResume', 'uuid-0')

    kwargs = {
        'connection_params': {
            'url': 'amqp://guest:guest@127.0.0.1:5672/'
        },
        'message_exchange': message_exchange,
        'task_exchange': task_exchange,
        'task_queue': task_queue,
    }
    try:
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

    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    launch()
