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

    for func in funcs + (lambda _: None, ):
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        history.append([state.started.done() for state in task_states])
        func(task_states)

    return history


def cancel(i):
    def func(tasks):
        tasks[i].done.cancel()
    return func


def complete(i):
    def func(tasks):
        tasks[i].done.set_result(None)
    return func


def async_test(func):
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper


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
    async def test_read_write_lock(self):
        lock = FifoLock()

        tasks = create_lock_tasks(
            lock,
            Write, Read, Read, Write, Read, Read)
        started_history = await mutate_tasks_in_sequence(
            tasks,
            complete(0), complete(2), complete(1), cancel(4), complete(3),
        )

        # Ensure only the first write has started...
        self.assertEqual(started_history[0], [True, False, False, False, False, False])

        # ... then only the next reads start...
        self.assertEqual(started_history[1], [True, True, True, False, False, False])

        # ... tasks finishing out of order doesn't change things...
        self.assertEqual(started_history[2], [True, True, True, False, False, False])

        # # ... a queued write starts only after all previous access finished...
        self.assertEqual(started_history[3], [True, True, True, True, False, False])

        # ... cancelling a task doesn't change things...
        self.assertEqual(started_history[4], [True, True, True, True, False, False])

        # ... and doesn't stop later tasks from running
        self.assertEqual(started_history[5], [True, True, True, True, True, True])

        # ... and a read requested during a read can proceed
        concurrently_added_started = await mutate_tasks_in_sequence(create_lock_tasks(
            lock, Read),
            complete(0),
        )
        self.assertEqual(concurrently_added_started[0], [True])

        complete(5)(tasks)

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
