import asyncio
import collections
import unittest

from fifolock import FifoLock


TaskState = collections.namedtuple('TaskState', ['started', 'done', 'task'])


def create_lock_tasks(*modes):
    async def access(mode, started, done):
        async with mode:
            started.set_result(None)
            await done

    def task(mode):
        started = asyncio.Future()
        done = asyncio.Future()
        task = asyncio.ensure_future(access(mode, started, done))
        return TaskState(started=started, done=done, task=task)

    return [task(mode) for mode in modes]


async def mutate_tasks_in_sequence(task_states, *funcs):
    history = []

    for func in funcs + (null, ):
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        history.append([state.started.done() for state in task_states])
        await func(task_states)

    return history


def cancel(i):
    async def func(tasks):
        tasks[i].task.cancel()
    return func


def complete(i):
    async def func(tasks):
        tasks[i].done.set_result(None)
    return func


def exception(i, exception):
    async def func(tasks):
        tasks[i].done.set_exception(exception)
    return func


async def null(_):
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

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Mutex), lock(Mutex)),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, False])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_mode_can_be_reused(self):

        lock = FifoLock()
        mode_instance = lock(Mutex)

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            mode_instance, mode_instance),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, False])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_mutex_raising_exception_bubbles_and_allows_later_mutex(self):

        lock = FifoLock()

        tasks = create_lock_tasks(
            lock(Mutex), lock(Mutex))
        exp = Exception('Raised exception')
        started_history = await mutate_tasks_in_sequence(
            tasks,
            exception(0, exp), complete(1),
        )

        self.assertEqual(tasks[0].task.exception(), exp)
        self.assertEqual(started_history[0], [True, False])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_mutex_cancelled_after_it_starts_allows_later_mutex(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Mutex), lock(Mutex), lock(Mutex)),
            complete(0), cancel(1), complete(2),
        )

        self.assertEqual(started_history[0], [True, False, False])
        self.assertEqual(started_history[1], [True, True, False])
        self.assertEqual(started_history[2], [True, True, True])

    @async_test
    async def test_mutex_cancelled_before_it_starts_allows_later_mutex(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Mutex), lock(Mutex), lock(Mutex)),
            cancel(1), complete(0), null, complete(2),
        )

        self.assertEqual(started_history[0], [True, False, False])
        self.assertEqual(started_history[1], [True, False, False])
        self.assertEqual(started_history[2], [True, False, True])

    @async_test
    async def test_mutex_requested_concurrently_can_start(self):

        lock = FifoLock()

        tasks_1 = create_lock_tasks(lock(Mutex))
        started_history_1 = await mutate_tasks_in_sequence(tasks_1)  # No mutation

        tasks_2 = create_lock_tasks(lock(Mutex))
        started_history_2 = await mutate_tasks_in_sequence(
            tasks_1 + tasks_2,
            complete(0), complete(1),
        )

        self.assertEqual(started_history_2[0], [True, False])
        self.assertEqual(started_history_2[1], [True, True])

    @async_test
    async def test_read_write_lock_write_blocks_write(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Write), lock(Write)),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, False])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_read_write_lock_write_blocks_read(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Write), lock(Read)),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, False])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_read_write_lock_read_allows_read(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Read), lock(Read)),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, True])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_read_write_lock_read_blocks_write(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Read), lock(Write)),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, False])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_read_write_lock_reads_complete_out_of_order_still_block_write(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Read), lock(Read), lock(Write)),
            complete(1), complete(0), complete(2),
        )

        self.assertEqual(started_history[0], [True, True, False])
        self.assertEqual(started_history[1], [True, True, False])
        self.assertEqual(started_history[2], [True, True, True])

    @async_test
    async def test_semaphore(self):
        lock = FifoLock()
        Semaphore = type('Semaphore', (SemaphoreBase, ), {'size': 2})

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock(Semaphore), lock(Semaphore), lock(Semaphore)),
            complete(1), complete(0), complete(2),
        )

        # Ensure only the first two have started...
        self.assertEqual(started_history[0], [True, True, False])

        # ... and one finishing allows the final to proceed
        self.assertEqual(started_history[1], [True, True, True])
