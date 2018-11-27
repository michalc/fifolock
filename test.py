import asyncio
import collections
import unittest

from fifolock import FifoLock


TaskState = collections.namedtuple('TaskState', ['acquired', 'done', 'task'])


def create_lock_tasks(*modes):
    async def access(mode, acquired, done):
        async with mode:
            acquired.set_result(None)
            await done

    def task(mode):
        acquired = asyncio.Future()
        done = asyncio.Future()
        task = asyncio.ensure_future(access(mode, acquired, done))
        return TaskState(acquired=acquired, done=done, task=task)

    return [task(mode) for mode in modes]


async def mutate_tasks_in_sequence(task_states, *funcs):
    history = []

    for func in funcs + (null, ):
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        history.append([state.acquired.done() for state in task_states])
        func(task_states)

    return history


def cancel(i):
    def func(tasks):
        tasks[i].task.cancel()
    return func


def complete(i):
    def func(tasks):
        tasks[i].done.set_result(None)
    return func


def exception(i, exception):
    def func(tasks):
        tasks[i].done.set_exception(exception)
    return func


def null(_):
    pass


def async_test(func):
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper


class Mutex(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Mutex]


class Read(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Write]


class Write(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Read] and not holds[Write]


class SemaphoreBase(asyncio.Future):
    @classmethod
    def is_compatible(cls, holds):
        return holds[cls] < cls.size


class TestFifoLock(unittest.TestCase):

    @async_test
    async def test_mutex_blocks_mutex(self):

        lock = FifoLock()

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Mutex), lock(Mutex)),
            complete(0), complete(1),
        )

        self.assertEqual(acquisition_history[0], [True, False])
        self.assertEqual(acquisition_history[1], [True, True])

    @async_test
    async def test_mode_can_be_reused(self):

        lock = FifoLock()
        mode_instance = lock(Mutex)

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            mode_instance, mode_instance),
            complete(0), complete(1),
        )

        self.assertEqual(acquisition_history[0], [True, False])
        self.assertEqual(acquisition_history[1], [True, True])

    @async_test
    async def test_mutex_raising_exception_bubbles_and_allows_later_mutex(self):

        lock = FifoLock()

        tasks = create_lock_tasks(
            lock(Mutex), lock(Mutex))
        exp = Exception('Raised exception')
        acquisition_history = await mutate_tasks_in_sequence(
            tasks,
            exception(0, exp), complete(1),
        )

        self.assertEqual(tasks[0].task.exception(), exp)
        self.assertEqual(acquisition_history[0], [True, False])
        self.assertEqual(acquisition_history[1], [True, True])

    @async_test
    async def test_mutex_cancelled_after_it_acquires_allows_later_mutex(self):

        lock = FifoLock()

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Mutex), lock(Mutex), lock(Mutex)),
            complete(0), cancel(1), complete(2),
        )

        self.assertEqual(acquisition_history[0], [True, False, False])
        self.assertEqual(acquisition_history[1], [True, True, False])
        self.assertEqual(acquisition_history[2], [True, True, True])

    @async_test
    async def test_mutex_cancelled_before_it_acquires_allows_later_mutex(self):

        lock = FifoLock()

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Mutex), lock(Mutex), lock(Mutex)),
            cancel(1), complete(0), null, complete(2),
        )

        self.assertEqual(acquisition_history[0], [True, False, False])
        self.assertEqual(acquisition_history[1], [True, False, False])
        self.assertEqual(acquisition_history[2], [True, False, True])

    @async_test
    async def test_mutex_requested_concurrently_can_acquire(self):

        lock = FifoLock()

        tasks_1 = create_lock_tasks(lock(Mutex))
        acquisition_history_1 = await mutate_tasks_in_sequence(tasks_1)  # No mutation

        tasks_2 = create_lock_tasks(lock(Mutex))
        acquisition_history_2 = await mutate_tasks_in_sequence(
            tasks_1 + tasks_2,
            complete(0), complete(1),
        )

        self.assertEqual(acquisition_history_2[0], [True, False])
        self.assertEqual(acquisition_history_2[1], [True, True])

    @async_test
    async def test_read_write_lock_write_blocks_write(self):

        lock = FifoLock()

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Write), lock(Write)),
            complete(0), complete(1),
        )

        self.assertEqual(acquisition_history[0], [True, False])
        self.assertEqual(acquisition_history[1], [True, True])

    @async_test
    async def test_read_write_lock_write_blocks_read(self):

        lock = FifoLock()

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Write), lock(Read)),
            complete(0), complete(1),
        )

        self.assertEqual(acquisition_history[0], [True, False])
        self.assertEqual(acquisition_history[1], [True, True])

    @async_test
    async def test_read_write_lock_read_allows_read(self):

        lock = FifoLock()

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Read), lock(Read)),
            complete(0), complete(1),
        )

        self.assertEqual(acquisition_history[0], [True, True])
        self.assertEqual(acquisition_history[1], [True, True])

    @async_test
    async def test_read_write_lock_read_blocks_write(self):

        lock = FifoLock()

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Read), lock(Write)),
            complete(0), complete(1),
        )

        self.assertEqual(acquisition_history[0], [True, False])
        self.assertEqual(acquisition_history[1], [True, True])

    @async_test
    async def test_read_write_lock_reads_complete_out_of_order_still_block_write(self):

        lock = FifoLock()

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Read), lock(Read), lock(Write)),
            complete(1), complete(0), complete(2),
        )

        self.assertEqual(acquisition_history[0], [True, True, False])
        self.assertEqual(acquisition_history[1], [True, True, False])
        self.assertEqual(acquisition_history[2], [True, True, True])

    @async_test
    async def test_read_write_lock_write_not_given_priority_over_read(self):

        lock = FifoLock()

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Write), lock(Read), lock(Write)),
            complete(0), complete(1), complete(2),
        )

        self.assertEqual(acquisition_history[0], [True, False, False])
        self.assertEqual(acquisition_history[1], [True, True, False])
        self.assertEqual(acquisition_history[2], [True, True, True])

    @async_test
    async def test_semaphore(self):
        lock = FifoLock()
        Semaphore = type('Semaphore', (SemaphoreBase, ), {'size': 2})

        acquisition_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Semaphore), lock(Semaphore), lock(Semaphore)),
            complete(1), complete(0), complete(2),
        )

        # Ensure only the first two have acquired...
        self.assertEqual(acquisition_history[0], [True, True, False])

        # ... and one finishing allows the final to proceed
        self.assertEqual(acquisition_history[1], [True, True, True])
