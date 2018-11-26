import asyncio
import collections
import unittest

from fifolock import FifoLock


TaskState = collections.namedtuple('TaskState', ['started', 'done', 'task'])


def create_lock_tasks(lock, *modes):
    async def access(mode, started, done):
        async with lock(mode):
            started.set_result(None)
            await done

    def task(mode):
        started = asyncio.Future()
        done = asyncio.Future()
        task = asyncio.create_task(access(mode, started, done))
        return TaskState(started=started, done=done, task=task)

    return [task(mode) for mode in modes]


async def mutate_tasks_in_sequence(task_states, *funcs):
    history = []

    async def null(_):
        pass

    for func in funcs + (null, ):
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        history.append([state.started.done() for state in task_states])
        await func(task_states)

    return history


def cancel(i):
    async def func(tasks):
        tasks[i].done.cancel()
    return func


def complete(i):
    async def func(tasks):
        tasks[i].done.set_result(None)
    return func


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
            lock,
            Mutex, Mutex),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, False])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_mutex_cancelled_after_it_starts_allows_later_mutex(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock,
            Mutex, Mutex, Mutex),
            complete(0), cancel(1), complete(2),
        )

        self.assertEqual(started_history[0], [True, False, False])
        self.assertEqual(started_history[1], [True, True, False])
        self.assertEqual(started_history[2], [True, True, True])

    @async_test
    async def test_mutex_cancelled_before_it_starts_allows_later_mutex(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock,
            Mutex, Mutex, Mutex),
            cancel(1), complete(0), complete(2),
        )

        self.assertEqual(started_history[0], [True, False, False])
        self.assertEqual(started_history[1], [True, False, False])
        self.assertEqual(started_history[2], [True, True, False])
        self.assertEqual(started_history[3], [True, True, True])

    @async_test
    async def test_mutex_requested_concurrently_can_start(self):

        lock = FifoLock()

        tasks_1 = create_lock_tasks(lock, Mutex)
        started_history_1 = await mutate_tasks_in_sequence(tasks_1)  # No mutation

        tasks_2 = create_lock_tasks(lock, Mutex)
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
            lock,
            Write, Write),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, False])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_read_write_lock_write_blocks_read(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock,
            Write, Read),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, False])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_read_write_lock_read_allows_read(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock,
            Read, Read),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, True])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_read_write_lock_read_blocks_write(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock,
            Read, Write),
            complete(0), complete(1),
        )

        self.assertEqual(started_history[0], [True, False])
        self.assertEqual(started_history[1], [True, True])

    @async_test
    async def test_read_write_lock_reads_complete_out_of_order_still_block_write(self):

        lock = FifoLock()

        started_history = await mutate_tasks_in_sequence(create_lock_tasks(
            lock,
            Read, Read, Write),
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
            lock,
            Semaphore, Semaphore, Semaphore),
            complete(1), complete(0), complete(2),
        )

        # Ensure only the first two have started...
        self.assertEqual(started_history[0], [True, True, False])

        # ... and one finishing allows the final to proceed
        self.assertEqual(started_history[1], [True, True, True])
