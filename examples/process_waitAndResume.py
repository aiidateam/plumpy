import plumpy
from tornado import ioloop, gen

class WaitForResumeProc(plumpy.Process):

    def run(self, **kwargs):
        print("Now I am running: {:}".format(self.state))
        return plumpy.Wait(self.after_watting)

    def after_watting(self):
        print("After resume from watting state: {:}".format(self.state))

if __name__ == "__main__":
    loop = ioloop.IOLoop()
    proc = WaitForResumeProc()
    print("state after inst: {:}".format(proc.state))  # Created

    @gen.coroutine
    def async_steps():
        print("state after running: {:}".format(proc.state))  # Watting

        yield proc.pause()
        print("Is paused?  {:}".format(proc.paused))

        proc.play()
        print("Is paused?  {:}".format(proc.paused))

        proc.resume()
        # Wait until the process is terminated
        yield proc.future()

        print("state done: {:}".format(proc.state))  # Finished

    loop.add_callback(proc.step_until_terminated)
    loop.run_sync(async_steps)

# output:
# state after inst: ProcessState.CREATED
# Now I am running: ProcessState.RUNNING
# state after running: ProcessState.WAITING
# Is paused?  True
# Is paused?  False
# After resume from watting state: ProcessState.RUNNING
# state done: ProcessState.FINISHED
