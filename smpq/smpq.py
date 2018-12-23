""" Simple multi-processing with queues
My first serious attempt at multiprocessing with Python ended in failure, in that
there was no significant speedup. I used multiprocessing.Pool to parallelize
the calculation of HHG spectra in benzene over different molecular angles. The
overhead of setting up the process each time (several seconds, worse on Windows)
prevented any improvement.

Following
https://pymotw.com/2/multiprocessing/communication.html
I wrote this simple system based on multiprocessing.Queue. The processes are
created once, and tasks passed from the main process to the worker processes using
the queue.

If tasks are defined in __main__, they cannot use anything from their enclosing 
scope - even modules. I didn't investigate this as I found that putting them in
a non-main module worked fine.

TODOs:
* get results during map rather than at the end to allow live display of results

Log levels:
    2: Several messages per task
    3: One message per task
"""
from typing import Callable
import multiprocessing, dill, queue, logging, warnings, time, signal
from operator import itemgetter
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class Worker(multiprocessing.Process):
    def __init__(self, number, tasks, results, protocol, retire_task_num):
        multiprocessing.Process.__init__(self)
        self.number = number
        self.tasks = tasks
        self.results = results
        self.protocol = protocol
        self.retire_task_num = retire_task_num
        self.num_tasks_done = 0

    def run(self):
        # On Linux, worker processes receive KeyboardInterrupt, which is not what
        # we want - cleanup is down my main process. See
        # http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            while self.retire_task_num is None or (self.num_tasks_done < self.retire_task_num):
                task_info = self.tasks.get()
                if task_info is None:
                    # Null task means close. Signal that this task is done, and exit
                    self.tasks.task_done()
                    logger.log(2, 'Worker process %s received None task, breaking from loop.', self.name)
                    break
                task_id, task_dumped, warning_filters = task_info
                logger.log(2, 'Worker process %s received task task_id %s.', self.name, task_id)
                try:
                    with warnings.catch_warnings():
                        for warning_filter in warning_filters:
                            warnings.filterwarnings(**warning_filter)
                        task = dill.loads(task_dumped)
                        value = task()
                        value_dumped = dill.dumps(value, self.protocol)
                except Exception as exception:
                    # Log the exception, but do not rethrow. Instead put the exception as the result of the task for the
                    # main process to deal with.
                    logger.exception(
                        'Worker process %s caught an exception during execution and pickling of results of task task_id %s.',
                        self.name, task_id)
                    value_dumped = dill.dumps(exception)
                self.num_tasks_done += 1
                self.results.put((self.number, task_id, value_dumped, self.num_tasks_done))
                self.tasks.task_done()
        except Exception:
            logger.exception('Worker process %s encountered an exception in run method.', self.name)
        self.results.close()
        logger.log(2, 'Worker %s run finishing normally, num_tasks_done=%d.', self.name, self.num_tasks_done)


class MapMultipleException(Exception):
    def __init__(self, exception_pairs):
        """
        Args:
            exceptions: sequence of (exception,index) tuples
        """
        ids, exceptions = zip(*exception_pairs)
        Exception.__init__(self, 'Exceptions raised by indices %s'%str(ids))
        self.ids = ids
        self.exceptions = exceptions


@contextmanager
def pool(num_workers, **kwargs):
    """Wrapper of Manager handling num_workers=None as trivial case."""
    if num_workers is None:
        yield map
    else:
        with Manager(num_workers, **kwargs) as pool:
            yield pool.map


@contextmanager
def pool_or_none(num_workers, **kwargs):
    """Context manager returning a pool object or None if no workers.

    Enables code like:

        with smpq.pool_or_none(num_workers) as pool:
            function_accepting_pool(pool)

    that works wtih num_workers=None or an integer, provided that function_accepting_pool accepts pool=None.
    """
    if num_workers is None:
        yield None
    else:
        with Manager(num_workers, **kwargs) as pool:
            yield pool


class Manager:
    def __init__(self, num_workers, sleep=time.sleep, protocol=None, retire_task_num:int=None):
        """Create queues and workers.

        Args:
            num_workers:
            sleep (function): sleep function while waiting for processes to do
                their thing. Accepts argument in seconds. Can pass something to
                e.g. keep GUIs interactive. For Qt this is QtTest.QTest.qWait
                (rememember that it accepts milliseconds).
            protocol: pickling protocol. None is interpreted by dill/pickle as default.
            retire_task_num: number of tasks after which workers 'retire'. Useful for dealing with slow memory
                leaks. It takes a few seconds to make a worker, so don't set this too low. None means no retirements.
        """
        self.tasks = multiprocessing.JoinableQueue()
        self.results = multiprocessing.Queue()
        self.num_workers = num_workers
        self.retire_task_num = retire_task_num
        self.protocol = protocol
        self.workers = [self.make_worker(number) for number in range(num_workers)]
        self.sleep = sleep
        self.num_tasks_added = 0
        self.num_results_got = 0

    def make_worker(self, number):
        worker = Worker(number, self.tasks, self.results, self.protocol, self.retire_task_num)
        worker.start()
        return worker

    # def update_workers(self):
    #     # Pick out active (not retired/terminated) workers.
    #     self.workers=[worker for worker in self.workers if worker.is_alive()]
    #     # Get new workers to bring the total to num_workers.
    #     num_create=self.num_workers-len(self.workers)
    #     if num_create>0:
    #         logger.log(3,'Creating %d workers.',num_create)
    #     for _ in range(num_create):
    #         worker=
    #         worker.start()
    #         self.workers.append(worker)

    def add_task(self, id, task, warning_filters=[]):
        t0 = time.time()
        task_dumped = dill.dumps(task, self.protocol)
        t1 = time.time()
        self.tasks.put((id, task_dumped, warning_filters))
        self.num_tasks_added += 1
        t2 = time.time()
        logger.log(3, 'Added task %s with dumped size %g MB. dumps took %g s, put took %g s. num_tasks_added=%d.', id,
                   len(task_dumped)/1e6, t1 - t0, t2 - t1, self.num_tasks_added)

    def map(self, func:Callable, args, warning_filters=[]):
        """Apply function series of arguments, yielding the results in order.
        
        Args:
            func (function): to be executed with each element of arg
            args (sequence or generator): arguments for func. The generator is only evaluated as necessary to save memory.
            warning_filters (list of dicts): each element is passed to warnings.
                filterwarnings in the child processes (since this state is not
                inherited from main process).
        """
        assert self.tasks.empty()
        last_log_time = -float('inf')
        log_wait_time = 0.1
        # Index of next result to yield, also number of results yielded.
        next_result_index = 0
        unyielded_results = []
        args = enumerate(args)
        num_tasks_added = 0
        while not (args is None and next_result_index == num_tasks_added):
            # Add tasks until queue has a reasonable number of tasks on it or we run out of tasks.
            while self.num_tasks_added - self.num_results_got < self.num_workers and args is not None:
                try:
                    index, arg = next(args)
                except StopIteration:
                    # All tasks are added. Now num_tasks_added is the total number of tasks.
                    args = None
                else:
                    self.add_task(index, lambda: func(arg), warning_filters)
                    num_tasks_added += 1

            # Get results from worker processes.
            while True:
                try:
                    index, value = self.get_result()
                except queue.Empty:
                    break
                # Add result to list of unyielded results
                unyielded_results.append((index, value))
                logger.log(2, 'Appended result of %d to unyielded results, whose length is now %d.', index,
                           len(unyielded_results))

            # Yield results in the correct order.
            while True:
                try:
                    results_i, value = next((i, value) for i, (result_index, value) in enumerate(unyielded_results) if
                                            result_index == next_result_index)
                except StopIteration:
                    break
                # Remove it from results
                unyielded_results.pop(results_i)
                logger.log(2, 'Yielding result of task %d.', next_result_index)
                next_result_index += 1
                yield value

            # Log if necessary
            now = time.time()
            if now - last_log_time > log_wait_time:
                last_log_time = now
                log_wait_time = min(log_wait_time*2, 20)
                logger.log(2, 'Waiting...')

            # Return control e.g. to GUI
            self.sleep(0.1)

        assert len(unyielded_results) == 0, len(unyielded_results)
        self.block_until_tasks_done()

    def block_until_tasks_done(self):
        logger.log(2, 'Calling self.tasks.join...')
        self.tasks.join()
        logger.log(2, 'self.tasks.join returned.')

    def close(self):
        logger.log(2, 'Signalling Manager close by sending a None task for each worker.')
        for _ in range(len(self.workers)):
            self.tasks.put(None)
        self.tasks.close()
        logger.log(2, 'Joining all self.workers...')
        for worker in self.workers:
            worker.join(0.5)
        # Sometimes this doesn't work i.e. on Windows with KeyboardInterrupt.
        for worker in self.workers:
            if worker.is_alive():
                logger.log(2, 'Worker %s is still alive, terminating...', worker.name)
                worker.terminate()
        logger.log(2, 'Manager.close complete.')

    def get_result(self):
        t0 = time.time()
        worker_number, task_id, value_dumped, tasks_done = self.results.get_nowait()
        self.num_results_got += 1
        t1 = time.time()
        value = dill.loads(value_dumped)
        if isinstance(value, Exception):
            logger.exception('Worker %d with returned an exception for task_id %s - raising.', worker_number, task_id)
            raise value
        t2 = time.time()
        logger.log(2, 'Got result task_id %s, size %g MB. get_nowait took %g s, loads took %g s.', task_id,
                   len(value_dumped)/1e6, t1 - t0, t2 - t1)
        assert self.retire_task_num is None or (tasks_done <= self.retire_task_num)
        if tasks_done == self.retire_task_num:
            # Worker is due for retirement.
            worker = self.workers[worker_number]
            logger.log(3, 'Worker %d due to retire, joining.', worker_number)
            worker.join()
            self.workers[worker_number] = self.make_worker(worker_number)
        return task_id, value

    def get_results(self):
        try:
            while True:
                yield self.get_result()
        except queue.Empty:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
